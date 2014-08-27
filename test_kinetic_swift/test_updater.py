import hashlib
import os
import random
import time

from swift.common.swob import Request
from swift.common.utils import Timestamp, split_path, hash_path

from kinetic_swift.obj import server, updater

from utils import (KineticSwiftTestCase, mocked_http_conn, FakeRing,
                   debug_logger)


class TestKineticObjectUpdater(KineticSwiftTestCase):

    def _get_path(self, **extra_parts):
        parts = {
            'host': 'localhost',
            'port': self.port,
            'part': 0,
            'account': 'a',
            'container': 'c',
            'object': self.buildKey('o'),
        }
        parts.update(extra_parts)
        path = '/%(host)s:%(port)s/%(part)s' % parts
        object_path = '/%(account)s/%(container)s/%(object)s' % parts
        return path + object_path

    def setUp(self):
        super(TestKineticObjectUpdater, self).setUp()
        self.port = self.ports[0]
        self.devices = 'localhost:%d' % self.port
        self.client = self.client_map[self.port]
        self.conf = {
            'unlink_wait': 'true',
        }
        self.app = server.app_factory(self.conf)
        self.policy = random.choice(list(server.diskfile.POLICIES))
        self.container_ring = FakeRing()
        self.logger = debug_logger()
        self.updater = updater.KineticUpdater(self.conf)
        self.updater.container_ring = self.container_ring
        self.updater.logger = self.logger

    def test_put_object_async_update(self):
        # put object
        req_timestamp = Timestamp(time.time())
        headers = {
            'X-Backend-Storage-Policy-Index': str(int(self.policy)),
            'x-timestamp': req_timestamp.internal,
            'content-type': 'application/octet-stream',
            'X-Container-Host': '10.0.0.1:6010',
            'X-Container-Device': 'sda1',
            'X-Container-Partition': '0',
        }
        body = 'test body'
        req = Request.blank(self._get_path(), method='PUT', headers=headers)
        req.body = body
        with mocked_http_conn(503) as fakeconn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)
        self.assertEqual(resp.status_int, 201)

        # check async keys
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))  # the async_update

        # check updater updates
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

        with mocked_http_conn(201, 201, 201,
                              give_connect=capture_updates) as fakeconn:
            self.updater.run_once(devices=self.devices)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)

        self.assertEqual(self.updater.stats, {
            'success': 1, 'found_updates': 1})
        self.assertEquals(self.container_ring.replicas,
                          len(container_updates))
        expected_headers = {
            'user-agent': 'obj-updater %d' % os.getpid(),
            'X-Size': str(len(body)),
            'X-Content-Type': 'application/octet-stream',
            'X-Etag': hashlib.md5(body).hexdigest(),
            'X-Timestamp': req_timestamp.internal,
            'X-Trans-Id': '-',
            'X-Backend-Storage-Policy-Index': str(int(self.policy)),
            'Referer': req.as_referer(),
        }
        expected_updates = [
            ('10.0.0.0', 1000, 'PUT', 'sda', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
            ('10.0.0.1', 1001, 'PUT', 'sdb', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
            ('10.0.0.2', 1002, 'PUT', 'sdc', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
        ]
        self.assertEqual(len(container_updates), len(expected_updates))
        for update, expected in zip(container_updates, expected_updates):
            ip, port, method, path, headers = update
            device, part, account, container, obj = split_path(
                path, minsegs=5, rest_with_last=True)
            update_parts = (ip, port, method, device, part, account,
                            container, obj, headers)
            self.assertEqual(update_parts, expected)

        # check async keys empty
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(0, len(keys))  # async update consumed

    def test_put_object_async_update_fails(self):
        # put object
        req_timestamp = Timestamp(time.time())
        headers = {
            'X-Backend-Storage-Policy-Index': str(int(self.policy)),
            'x-timestamp': req_timestamp.internal,
            'content-type': 'application/octet-stream',
            'X-Container-Host': '10.0.0.1:6010',
            'X-Container-Device': 'sda1',
            'X-Container-Partition': '0',
        }
        body = 'test body'
        req = Request.blank(self._get_path(), method='PUT', headers=headers)
        req.body = body
        with mocked_http_conn(503) as fakeconn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)
        self.assertEqual(resp.status_int, 201)

        # check async keys
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))  # the async_update

        # check updater updates
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

        with mocked_http_conn(201, 201, 503,
                              give_connect=capture_updates) as fakeconn:
            self.updater.run_once(devices=self.devices)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)

        self.assertEqual(self.updater.stats, {
            'failures': 1, 'found_updates': 1})
        self.assertEquals(self.container_ring.replicas,
                          len(container_updates))
        expected_headers = {
            'user-agent': 'obj-updater %d' % os.getpid(),
            'X-Size': str(len(body)),
            'X-Content-Type': 'application/octet-stream',
            'X-Etag': hashlib.md5(body).hexdigest(),
            'X-Timestamp': req_timestamp.internal,
            'X-Trans-Id': '-',
            'X-Backend-Storage-Policy-Index': str(int(self.policy)),
            'Referer': req.as_referer(),
        }
        expected_updates = [
            ('10.0.0.0', 1000, 'PUT', 'sda', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
            ('10.0.0.1', 1001, 'PUT', 'sdb', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
            ('10.0.0.2', 1002, 'PUT', 'sdc', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
        ]
        self.assertEqual(len(container_updates), len(expected_updates))
        for update, expected in zip(container_updates, expected_updates):
            ip, port, method, path, headers = update
            device, part, account, container, obj = split_path(
                path, minsegs=5, rest_with_last=True)
            update_parts = (ip, port, method, device, part, account,
                            container, obj, headers)
            self.assertEqual(update_parts, expected)

        # check async keys sticks around
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))  # async update consumed

        # run updater again, only last update is sent
        container_updates = []
        with mocked_http_conn(201,
                              give_connect=capture_updates) as fakeconn:
            self.updater.run_once(devices=self.devices)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)

        self.assertEqual(self.updater.stats, {
            'failures': 1, 'success': 1, 'found_updates': 2})
        expected_updates = [
            ('10.0.0.2', 1002, 'PUT', 'sdc', '1', 'a', 'c',
             self.buildKey('o'), expected_headers),
        ]
        self.assertEqual(len(container_updates), len(expected_updates))
        for update, expected in zip(container_updates, expected_updates):
            ip, port, method, path, headers = update
            device, part, account, container, obj = split_path(
                path, minsegs=5, rest_with_last=True)
            update_parts = (ip, port, method, device, part, account,
                            container, obj, headers)
            self.assertEqual(update_parts, expected)

        # check async keys empty
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(0, len(keys))  # async update consumed
