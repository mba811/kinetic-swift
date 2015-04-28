# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import time
import random
import os
import hashlib
import email

from swift.common.swob import Request
from swift.common.utils import Timestamp, split_path, hash_path

from kinetic_swift.obj import server

from utils import KineticSwiftTestCase, mocked_http_conn


class TestKineticObjectServer(KineticSwiftTestCase):

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
        super(TestKineticObjectServer, self).setUp()
        self.port = self.ports[0]
        self.client = self.client_map[self.port]
        self.disk_chunk_size = 100
        self.conf = {
            'unlink_wait': 'true',
            'disk_chunk_size': self.disk_chunk_size,
        }
        self.app = server.app_factory(self.conf)
        self.policy = random.choice(list(server.diskfile.POLICIES))

    def test_get_missing_404(self):
        req = Request.blank(self._get_path())
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 404)

    def test_put_and_get(self):
        # put object
        headers = {
            'x-timestamp': Timestamp(time.time()).internal,
            'content-type': 'application/octet-stream',
        }
        body = 'test body'
        req = Request.blank(self._get_path(), method='PUT', headers=headers)
        req.body = body
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

        # get object
        req = Request.blank(self._get_path())
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, body)

    def test_put_and_get_range(self):
        # put object
        headers = {
            'x-timestamp': Timestamp(time.time()).internal,
            'content-type': 'application/octet-stream',
        }
        body = 'test body'
        req = Request.blank(self._get_path(), method='PUT', headers=headers)
        req.body = body
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

        # get object with range
        req = Request.blank(self._get_path())
        req.range = 'bytes=0-4'
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 206)
        self.assertEqual(resp.body, 'test ')

        # get object with offset
        req = Request.blank(self._get_path())
        req.range = 'bytes=4-'
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 206)
        self.assertEqual(resp.body, ' body')

    def test_put_and_get_multirange(self):
        # put object
        headers = {
            'x-timestamp': Timestamp(time.time()).internal,
            'content-type': 'application/octet-stream',
        }
        req = Request.blank(self._get_path(), method='PUT', headers=headers)
        num_chunks = 10
        req.body = ''.join(chr(i + 97) * self.disk_chunk_size for i in
                           range(num_chunks))
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

        # get object with range
        req = Request.blank(self._get_path())
        req.range = 'bytes=301-455,686-792'
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 206)
        msg = email.message_from_string(
            'Content-Type: %s\r\n' % resp.headers['Content-Type'] + resp.body)
        parts = [p for p in msg.walk()][1:]
        self.assertEqual(2, len(parts))
        self.assertEqual(parts[0].get_payload(), 'd' * 99 + 'e' * 56)
        self.assertEqual(parts[1].get_payload(), 'g' * 14 + 'h' * 93)

    def test_put_container_update(self):
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

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
        with mocked_http_conn(201, give_connect=capture_updates) as fakeconn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)
        self.assertEqual(resp.status_int, 201)

        self.assertEqual(len(container_updates), 1)
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '6010')
            self.assertEqual(method, 'PUT')
            device, part, account, container, obj = split_path(
                path, minsegs=5, rest_with_last=True)
            self.assertEqual(device, 'sda1')
            self.assertEqual(part, '0')
            self.assertEqual(account, 'a')
            self.assertEqual(container, 'c')
            self.assertEqual(obj, self.buildKey('o'))
            expected = {
                'User-Agent': 'object-server %d' % os.getpid(),
                'X-Size': str(len(body)),
                'X-Content-Type': 'application/octet-stream',
                'X-Etag': hashlib.md5(body).hexdigest(),
                'X-Timestamp': req_timestamp.internal,
                'X-Trans-Id': '-',
                'X-Backend-Storage-Policy-Index': str(int(self.policy)),
                'Referer': req.as_referer(),
            }
            self.assertEqual(headers, expected)

    def test_put_container_update_fails(self):
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
        async_key = keys[0]
        expected = start_key + '.%s' % req_timestamp.internal
        self.assertEqual(expected, async_key)
        resp = self.client.get(expected)
        async_data = server.msgpack.unpackb(resp.wait().value)
        expected = {
            'account': 'a',
            'container': 'c',
            'obj': self.buildKey('o'),
            'op': 'PUT',
            'headers': {
                'User-Agent': 'object-server %d' % os.getpid(),
                'X-Size': str(len(body)),
                'X-Content-Type': 'application/octet-stream',
                'X-Etag': hashlib.md5(body).hexdigest(),
                'X-Timestamp': req_timestamp.internal,
                'X-Trans-Id': '-',
                'X-Backend-Storage-Policy-Index': str(int(self.policy)),
                'Referer': req.as_referer(),
            },
        }
        self.assertEqual(async_data, expected)

    def test_delete_container_update(self):
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

        # delete object
        req_timestamp = Timestamp(time.time())
        headers = {
            'x-timestamp': req_timestamp.internal,
            'content-type': 'application/octet-stream',
            'X-Backend-Storage-Policy-Index': int(self.policy),
            'X-Container-Host': '10.0.0.1:6010',
            'X-Container-Device': 'sda1',
            'X-Container-Partition': '0',
        }
        req = Request.blank(self._get_path(), method='DELETE', headers=headers)
        with mocked_http_conn(204, give_connect=capture_updates) as fakeconn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)
        self.assertEqual(resp.status_int, 404)

        self.assertEqual(len(container_updates), 1)
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '6010')
            self.assertEqual(method, 'DELETE')
            device, part, account, container, obj = split_path(
                path, minsegs=5, rest_with_last=True)
            self.assertEqual(device, 'sda1')
            self.assertEqual(part, '0')
            self.assertEqual(account, 'a')
            self.assertEqual(container, 'c')
            self.assertEqual(obj, self.buildKey('o'))
            expected = {
                'Referer': req.as_referer(),
                'X-Backend-Storage-Policy-Index': str(int(self.policy)),
                'X-Timestamp': req_timestamp.internal,
                'User-Agent': 'object-server %d' % os.getpid(),
                'X-Trans-Id': '-',
            }
            self.assertEqual(headers, expected)

    def test_delete_container_update_fails(self):
        # delete object
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
        req = Request.blank(self._get_path(), method='DELETE',
                            headers=headers)
        req.body = body
        with mocked_http_conn(503) as fakeconn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, next, fakeconn.code_iter)
        self.assertEqual(resp.status_int, 404)

        # check async keys
        storage_policy = server.diskfile.get_async_dir(int(self.policy))
        hashpath = hash_path('a', 'c', self.buildKey('o'))
        start_key = '%s.%s' % (storage_policy, hashpath)
        end_key = '%s.%s/' % (storage_policy, hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))  # the async_update
        async_key = keys[0]
        expected = start_key + '.%s' % req_timestamp.internal
        self.assertEqual(expected, async_key)
        resp = self.client.get(expected)
        async_data = server.msgpack.unpackb(resp.wait().value)
        expected = {
            'account': 'a',
            'container': 'c',
            'obj': self.buildKey('o'),
            'op': 'DELETE',
            'headers': {
                'User-Agent': 'object-server %d' % os.getpid(),
                'X-Timestamp': req_timestamp.internal,
                'X-Trans-Id': '-',
                'X-Backend-Storage-Policy-Index': str(int(self.policy)),
                'Referer': req.as_referer(),
            },
        }
        self.assertEqual(async_data, expected)


if __name__ == "__main__":
    unittest.main()
