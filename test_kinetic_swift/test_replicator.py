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

from collections import defaultdict
from contextlib import closing
import cPickle as pickle
import gzip
import itertools
import os
import time
import random
import unittest
import uuid
import eventlet
from StringIO import StringIO
from ConfigParser import ConfigParser

import mock

from swift.common import ring, storage_policy, utils as swift_utils

from kinetic_swift.obj import replicator, server
from kinetic_swift.utils import ic_conf_body, key_range_markers

import utils


def create_rings(data_dir, *ports):
    """
    This makes a two replica, four part ring.
    """
    devices = []
    for port in ports:
        devices.append({
            'id': 0,
            'zone': 0,
            'device': '127.0.0.1:%s' % port,
            'ip': '127.0.0.1',
            'port': port + 1,
        })

    def iter_devices():
        if not devices:
            return
        while True:
            for dev_id in range(len(devices)):
                yield dev_id

    device_id_gen = iter_devices()
    replica2part2device = ([], [])
    for part in range(4):
        for replica in range(2):
            dev_id = device_id_gen.next()
            replica2part2device[replica].append(dev_id)
    ring_data = ring.RingData(replica2part2device, devices, 30)

    for policy in storage_policy.POLICIES:
        object_ring_path = os.path.join(
            data_dir, policy.ring_name + '.ring.gz')
        with closing(gzip.GzipFile(object_ring_path, 'wb')) as f:
            ring_data = ring.RingData(replica2part2device, devices, 30)
            pickle.dump(ring_data, f)


class TestUtilFunctions(unittest.TestCase):

    def test_split_key(self):
        hash_ = swift_utils.hash_path('a', 'c', 'o')
        t = swift_utils.Timestamp(time.time())
        nonce = uuid.uuid4()
        key = 'objects.%s.%s.data.%s' % (hash_, t.internal, nonce)
        expected = {
            'hashpath': hash_,
            'frag_index': None,
            'nonce': str(nonce),
            'policy': storage_policy.POLICIES.legacy,
            'timestamp': t.internal,
            'ext': 'data',
        }
        try:
            self.assertEqual(replicator.split_key(key), expected)
        except AssertionError as e:
            msg = '%s for key %r' % (e, key)
            self.fail(msg)

    def test_multiple_polices(self):
        hash_ = swift_utils.hash_path('a', 'c', 'o')
        t = swift_utils.Timestamp(time.time())
        nonce = uuid.uuid4()
        key_template = '%(prefix)s.%(hash)s.%(timestamp)s.%(ext)s' \
            '.%(nonce)s%(trailing_frag)s'
        with utils.patch_policies(with_ec_default=True):
            for p in storage_policy.POLICIES:
                if p.policy_type == storage_policy.EC_POLICY:
                    frag_index = random.randint(0, 10)
                    trailing_frag = '-%d' % (frag_index)
                else:
                    frag_index = None
                    trailing_frag = ''
                ext = random.choice(['data', 'ts'])
                object_prefix = storage_policy.get_policy_string('objects', p)
                key = key_template % {
                    'prefix': object_prefix,
                    'hash': hash_,
                    'timestamp': t.internal,
                    'ext': ext,
                    'nonce': nonce,
                    'trailing_frag': trailing_frag,
                }
                expected = {
                    'hashpath': hash_,
                    'policy': p,
                    'nonce': str(nonce),
                    'frag_index': frag_index,
                    'timestamp': t.internal,
                    'ext': ext,
                }
                try:
                    self.assertEqual(replicator.split_key(key), expected)
                except AssertionError as e:
                    msg = '%s\n\n ... for key %r' % (e, key)
                    self.fail(msg)


@utils.patch_policies(with_ec_default=False)
class TestKineticReplicator(utils.KineticSwiftTestCase):

    REPLICATION_MODE = 'push'
    PORTS = (9010, 9020, 9030)

    def setUp(self):
        super(TestKineticReplicator, self).setUp()
        create_rings(self.test_dir, *self.ports)
        recon_cache_path = os.path.join(self.test_dir, 'recon_cache')
        os.makedirs(recon_cache_path)
        conf = {
            'swift_dir': self.test_dir,
            'kinetic_replication_mode': self.REPLICATION_MODE,
            'recon_cache_path': recon_cache_path,
            'disk_chunk_size': 100,
        }
        self.daemon = replicator.KineticReplicator(conf)
        # force ring reload
        for policy in server.diskfile.POLICIES:
            policy.object_ring = None
            self.daemon.load_object_ring(policy)
        self.logger = self.daemon.logger = \
            utils.debug_logger('test-kinetic-replicator')
        self._df_router = server.diskfile.DiskFileRouter(
            {'unlink_wait': True}, self.logger)
        self.policy = random.choice(list(server.diskfile.POLICIES))
        self.mgr = self._df_router[self.policy]

    def put_object(self, device, object_name, body='', timestamp=None,
                   policy=None):
        if isinstance(body, basestring):
            body = [body]
        policy = policy or self.policy
        df = self.mgr.get_diskfile(device, '0', 'a', 'c', object_name,
                                   policy=policy)
        metadata = {'X-Timestamp': timestamp or time.time()}
        with df.create() as writer:
            for chunk in body:
                writer.write(chunk)
            writer.put(metadata)
        return metadata, ''.join(body)

    def delete_obj(self, device, object_name, timestamp=None, policy=None):
        policy = policy or self.policy
        df = self.mgr.get_diskfile(device, '0', 'a', 'c', object_name,
                                   policy=policy)
        df.delete(timestamp or time.time())

    def get_object(self, device, object_name, policy=None):
        policy = policy or self.policy
        df = self.mgr.get_diskfile(device, '0', 'a', 'c', object_name,
                                   policy=policy)
        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)
        return metadata, body

    def test_setup(self):
        self.assertEqual(self.daemon.replication_mode, self.REPLICATION_MODE)
        self.assertEqual(self.daemon.swift_dir, self.test_dir)
        expected_path = os.path.join(self.test_dir, 'object.ring.gz')
        object_ring = self.daemon.load_object_ring(
            server.diskfile.POLICIES.legacy)
        self.assertEqual(object_ring.serialized_path, expected_path)
        self.assertEquals(len(object_ring.devs), len(self.ports))
        for client in self.client_map.values():
            resp = client.getKeyRange('chunks.', 'objects/')
            self.assertEquals(resp.wait(), [])

    def test_iter_all_objects(self):
        port = self.ports[0]
        dev = '127.0.0.1:%s' % port
        for policy in server.diskfile.POLICIES:
            self.put_object(dev, 'obj1', policy=policy)
        conn = self.client_map[port]
        for policy in server.diskfile.POLICIES:
            keys = list(self.daemon.iter_all_objects(conn, policy))
            self.assertEqual(1, len(keys))

    def test_iter_lots_of_objects(self):
        port = self.ports[0]
        dev = '127.0.0.1:%s' % port
        for policy in server.diskfile.POLICIES:
            for i in range(20):
                self.put_object(dev, 'obj-%s' % i, policy=policy)
        conn = self.client_map[port]
        conn.maxReturned = 3
        for policy in server.diskfile.POLICIES:
            keys = list(self.daemon.iter_all_objects(conn, policy))
            self.assertEqual(20, len(keys))

    def test_reclaim_tombstones(self):
        port = self.ports[0]
        dev = '127.0.0.1:%s' % port
        ts = (server.diskfile.Timestamp(t) for t in
              itertools.count(int(time.time()) - 1000))
        conn = self.client_map[port]
        # create two objects
        self.put_object(dev, 'obj1', timestamp=next(ts).internal)
        self.put_object(dev, 'obj2', timestamp=next(ts).internal)
        keys = list(self.daemon.iter_all_objects(conn, self.policy))
        self.assertEqual(2, len(keys))
        # delete one
        self.delete_obj(dev, 'obj2', timestamp=next(ts).internal)
        keys = list(self.daemon.iter_all_objects(conn, self.policy))
        self.assertEqual(2, len(keys))
        # bring in reclaim age
        self.daemon.reclaim_age = 500
        keys = list(self.daemon.iter_all_objects(conn, self.policy))
        self.assertEqual(1, len(keys))
        # sanity
        resp = conn.getKeyRange('chunks.', 'objects/')
        self.assertEqual(len(resp.wait()), 1)

    def test_replicate_all_policies(self):
        self.daemon.run_once()
        found_storage_policies = set()
        for msg in self.daemon.logger.get_lines_for_level('debug'):
            if 'begin replication' not in msg.lower():
                continue
            for policy in storage_policy.POLICIES:
                if str(policy) in msg:
                    found_storage_policies.add(policy)
        self.assertEqual(found_storage_policies, set(storage_policy.POLICIES))

    def test_do_not_replicate_other_policy(self):
        other_policy = random.choice([
            p for p in storage_policy.POLICIES
            if p != self.policy])
        # put an object in the "other" policy
        other_dev = random.choice(other_policy.object_ring.devs)
        self.put_object(other_dev['device'], 'obj1', policy=other_policy)
        # run replication
        self.daemon.run_once()
        # make sure no copies found their way into our policy
        for dev in self.policy.object_ring.devs:
            self.assertRaises(server.diskfile.DiskFileNotExist,
                              self.get_object, dev['device'], 'obj1',
                              policy=self.policy)
        # and now we should have all three in other policy
        replicas = 0
        for dev in self.policy.object_ring.devs:
            try:
                self.get_object(dev['device'], 'obj1', policy=other_policy)
            except server.diskfile.DiskFileNotExist:
                continue
            replicas += 1
        self.assertEqual(other_policy.object_ring.replica_count, replicas)

    def test_replicate_one_object(self):
        source_port = self.ports[0]
        source_device = '127.0.0.1:%s' % source_port
        source_client = self.client_map[source_port]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        target_client = self.client_map[target_port]
        expected = self.put_object(source_device, 'obj1')
        self.daemon._replicate(source_device, policy=self.policy)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        source_resp = source_client.getKeyRange('chunks.', 'objects/')
        target_resp = target_client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        target_keys = target_resp.wait()
        self.assertEquals(source_keys, target_keys)
        self.assertEqual(len(self.daemon._conn_pool),
                         self.policy.object_ring.replica_count)

    def test_cleanup_aborted_uploads(self):
        port = random.choice(self.ports)
        conn = self.client_map[port]
        dev = '127.0.0.1:%s' % port
        num_chunks = 3

        # first make a good obj1 diskfile
        def good_body():
            for i in range(num_chunks):
                yield chr(97 + i) * self.mgr.disk_chunk_size
        self.put_object(dev, 'obj1', body=good_body())

        # sanity
        metadata, body = self.get_object(dev, 'obj1')
        self.assertEqual(body, ''.join(good_body()))

        # sanity chunks
        keys = conn.getKeyRange('chunks.', 'chunks/').wait()
        self.assertEqual(3, len(keys))

        # now upload another but blow up at the end
        def exploding_body():
            for chunk in good_body():
                yield chunk
            raise Exception('KABOOM!')

        try:
            self.put_object(dev, 'obj1', body=exploding_body())
        except Exception:
            pass
        else:
            self.fail('exploding_body did not explode!')

        # can still read old version
        self.assertEqual(self.get_object(dev, 'obj1'), (metadata, body))

        # ... but there's a few extra chunks
        extra_chunks = []
        for key in conn.getKeyRange('chunks.', 'chunks/').wait():
            if key in keys:
                continue
            extra_chunks.append(key)
        self.assertTrue(extra_chunks)
        hash_, nonce = extra_chunks[0].split('.')[1:3]

        # ... and a temp_marker
        tmp = replicator.diskfile.get_tmp_dir(self.policy)
        temp_markers = []
        for key in conn.getKeyRange(*key_range_markers(tmp)).wait():
            temp_markers.append(key)
        self.assertEqual(1, len(temp_markers))  # sanity

        # which points to the extra chunk keys by hash and nonce
        temp_hash, temp_nonce = temp_markers[0].split('.')[1:3]
        self.assertEqual(hash_, temp_hash)
        self.assertEqual(nonce, temp_nonce)
        extra_chunk_marker = 'chunks.%s.%s' % (hash_, nonce)

        # ... it'll stay like this until the aborted upload times out
        self.daemon._replicate(dev, policy=self.policy)
        expected = {
            extra_chunk_marker: len(extra_chunks),
            tmp: 1,
        }
        for marker, count in expected.items():
            keys = conn.getKeyRange(*key_range_markers(marker)).wait()
            msg = 'expected %s %s keys, found %r' % (count, marker, keys)
            self.assertEqual(count, len(keys), msg)

        # ... but after awhile, next time it runs
        the_future = time.time() + (9 * 60 * 60)
        with mock.patch('time.time') as mock_time:
            mock_time.return_value = the_future
            self.daemon._replicate(dev, policy=self.policy)

        expected = {
            extra_chunk_marker: 0,
            tmp: 0,
        }
        for marker, count in expected.items():
            keys = conn.getKeyRange(*key_range_markers(marker)).wait()
            msg = 'expected %s keys, found %r' % (count, keys)
            self.assertEqual(count, len(keys), msg)

    def test_cleanup_orphaned_temp_markers(self):
        # use the first primary to keep the object from being replicated off
        port = self.ports[0]
        conn = self.client_map[port]
        dev = '127.0.0.1:%s' % port
        num_chunks = 3

        # we're going to sniff for temp_markers during upload
        tmp = replicator.diskfile.get_tmp_dir(self.policy)
        temp_markers = []

        def find_temp_markers():
            for key in conn.getKeyRange(*key_range_markers(tmp)).wait():
                temp_markers.append(key)

        # sanity check no temp markers
        find_temp_markers()
        self.assertFalse(temp_markers)

        # this is our test body
        def good_body():
            for i in range(num_chunks):
                yield chr(97 + i) * self.mgr.disk_chunk_size

        # make a new object, and sniff for temp_markers as we go
        def sniffing_body():
            for chunk in good_body():
                yield chunk
            find_temp_markers()
            yield ''

        self.put_object(dev, 'obj1', body=sniffing_body())

        # sanity object is fine
        metadata, body = self.get_object(dev, 'obj1')
        self.assertEqual(body, ''.join(good_body()))

        # temp_marker is of course cleaned up
        self.assertEqual(len(temp_markers), 1)
        temp_marker = temp_markers.pop(0)
        self.assertFalse(conn.get(temp_marker).wait())

        # ... but even if we put it back to simulate a failure to remove the
        # temp_marker after a successful upload
        conn.put(temp_marker, '').wait()

        # replication will ignore it
        self.daemon._replicate(dev, policy=self.policy)

        expected = {
            'chunks': 3,
            tmp: 1,
        }
        for marker, count in expected.items():
            keys = conn.getKeyRange(*key_range_markers(marker)).wait()
            self.assertEqual(count, len(keys))

        # ... and the object is uneffected
        self.assertEqual(self.get_object(dev, 'obj1'), (metadata, body))

        # ... until sometime later, replication will just clean it up
        the_future = time.time() + (9 * 60 * 60)
        with mock.patch('time.time') as mock_time:
            mock_time.return_value = the_future
            self.daemon._replicate(dev, policy=self.policy)

        expected = {
            'chunks': 3,
            tmp: 0,
        }
        for marker, count in expected.items():
            keys = conn.getKeyRange(*key_range_markers(marker)).wait()
            self.assertEqual(count, len(keys))

        # ... and the object is *still* uneffected
        self.assertEqual(self.get_object(dev, 'obj1'), (metadata, body))

    def test_replicate_to_right_servers(self):
        source_device = '127.0.0.1:%s' % self.ports[0]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        other_port = self.ports[2]
        other_device = '127.0.0.1:%s' % other_port

        expected = self.put_object(source_device, 'obj1')
        self.daemon._replicate(source_device, policy=self.policy)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        self.assertRaises(server.diskfile.DiskFileNotExist, self.get_object,
                          other_device, 'obj1')

    def test_replicate_random_chunks(self):
        object_ring = self.policy.object_ring
        _part, devices = object_ring.get_nodes('a', 'c', 'random_object')
        self.assertEquals(2, len(devices))
        source_device, target_device = [d['device'] for d in devices]
        object_size = 2 ** 20
        body = '\x00' * object_size
        expected = self.put_object(source_device, 'random_object', body=body)
        self.daemon._replicate(source_device, policy=self.policy)
        result = self.get_object(target_device, 'random_object')
        self.assertEquals(result, expected)

    def test_nonce_reconciliation(self):
        source_port = self.ports[0]
        source_device = '127.0.0.1:%s' % source_port
        source_client = self.client_map[source_port]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        target_client = self.client_map[target_port]
        # two requests with identical timestamp and different nonce
        req_timestamp = time.time()
        self.put_object(source_device, 'obj1', timestamp=req_timestamp)
        self.put_object(target_device, 'obj1', timestamp=req_timestamp)
        source_meta, _body = self.get_object(source_device, 'obj1')
        target_meta, _body = self.get_object(target_device, 'obj1')
        self.assertNotEqual(source_meta, target_meta)
        source_nonce = source_meta.pop('X-Kinetic-Chunk-Nonce')
        target_nonce = target_meta.pop('X-Kinetic-Chunk-Nonce')
        self.assertNotEqual(source_nonce, target_nonce)
        self.assertEqual(source_meta, target_meta)
        # peek at keys
        source_resp = source_client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        target_resp = target_client.getKeyRange('chunks.', 'objects/')
        target_keys = target_resp.wait()
        self.assertEqual(len(source_keys), len(target_keys))
        for source_key, target_key in zip(source_keys, target_keys):
            source_key_info = replicator.split_key(source_key)
            target_key_info = replicator.split_key(target_key)
            for key in source_key_info:
                if key == 'nonce':
                    continue
                self.assertEqual(source_key_info[key], target_key_info[key])
            self.assertNotEqual(source_key_info['nonce'],
                                target_key_info['nonce'])
        original_key_count = len(source_keys)
        # perform replication, should more or less no-op
        self.daemon._replicate(source_device, policy=self.policy)
        source_resp = source_client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        target_resp = target_client.getKeyRange('chunks.', 'objects/')
        target_keys = target_resp.wait()
        self.assertEqual(len(source_keys), len(target_keys))
        for source_key, target_key in zip(source_keys, target_keys):
            source_key_info = replicator.split_key(source_key)
            target_key_info = replicator.split_key(target_key)
            for key in source_key_info:
                if key == 'nonce':
                    continue
                self.assertEqual(source_key_info[key], target_key_info[key])
            self.assertNotEqual(source_key_info['nonce'],
                                target_key_info['nonce'])
        post_replication_key_count = len(source_keys)
        self.assertEquals(original_key_count, post_replication_key_count)

    def test_replicate_cleans_up_handoffs(self):
        source_device = '127.0.0.1:%s' % self.ports[0]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        other_port = self.ports[2]
        other_device = '127.0.0.1:%s' % other_port

        # put a copy on the handoff
        expected = self.put_object(other_device, 'obj1')
        # replicate to other servers
        self.daemon.run_once()
        result = self.get_object(source_device, 'obj1')
        self.assertEquals(expected, result)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        # and now it's gone from handoff
        self.assertRaises(server.diskfile.DiskFileNotExist, self.get_object,
                          other_device, 'obj1')

    def test_replicate_handoff_overwrites_old_version(self):
        ts = (server.diskfile.Timestamp(t) for t in
              itertools.count(int(time.time())))
        source_device = '127.0.0.1:%s' % self.ports[0]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        other_port = self.ports[2]
        other_device = '127.0.0.1:%s' % other_port

        # put an old copy on source
        self.put_object(source_device, 'obj1',
                        timestamp=ts.next().internal)
        # put a newer copy on the handoff
        expected = self.put_object(other_device, 'obj1',
                                   timestamp=ts.next().internal)
        # replicate to other servers
        self.daemon._replicate(other_device, policy=self.policy)
        result = self.get_object(source_device, 'obj1')
        self.assertEquals(expected, result)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        # and now it's gone from handoff
        self.assertRaises(server.diskfile.DiskFileNotExist, self.get_object,
                          other_device, 'obj1')


class TestKineticCopyReplicator(TestKineticReplicator):

    REPLICATION_MODE = 'copy'


def create_ec_rings(data_dir, obj_port, *ports):
    """
    This makes a four node, four part ring.
    """

    devices = []
    for port in ports:
        devices.append({
            'id': 0,
            'zone': 0,
            'ip': '127.0.0.1',
            'port': obj_port,
            'device': '127.0.0.1:%s' % port,
        })

    def iter_devices():
        if not devices:
            return
        while True:
            for dev_id in range(len(devices)):
                yield dev_id

    device_id_gen = iter_devices()
    replica2part2device = ([], [], [])
    for part in range(4):
        for replica in range(3):
            dev_id = device_id_gen.next()
            replica2part2device[replica].append(dev_id)

    for policy in storage_policy.POLICIES:
        object_ring_path = os.path.join(
            data_dir, policy.ring_name + '.ring.gz')
        with closing(gzip.GzipFile(object_ring_path, 'wb')) as f:
            ring_data = ring.RingData(replica2part2device, devices, 30)
            pickle.dump(ring_data, f)


def create_replicated_ring_data(port, *devices):
    """
    Three device, single port, two replica ring.
    """
    ring_devices = []
    for i, device in enumerate(devices):
        ring_devices.append({
            'id': i,
            'zone': 0,
            'device': device,
            'ip': '127.0.0.1',
            'port': port,
        })

    def iter_devices():
        if not ring_devices:
            return
        while True:
            for dev_id in range(len(ring_devices)):
                yield dev_id

    device_id_gen = iter_devices()
    replica2part2device = ([], [])
    for part in range(4):
        for replica in range(2):
            dev_id = device_id_gen.next()
            replica2part2device[replica].append(dev_id)

    return ring.RingData(replica2part2device, ring_devices, 30)


from swift.account import server as account_server
from swift.container import server as container_server


def create_ac_servers(data_dir):
    controller_map = {
        'account': account_server.AccountController,
        'container': container_server.ContainerController,
    }
    devices = ['sda', 'sdb', 'sdc']
    device_dir = os.path.join(data_dir, 'devs')
    os.makedirs(device_dir)
    conf = {
        'swift_dir': data_dir,
        'devices': device_dir,
        'mount_check': False,
    }
    servers = []
    for server_type, controller_class in controller_map.items():
        socket = eventlet.listen(('127.0.0.1', 0))
        port = socket.getsockname()[1]
        ring_path = os.path.join(data_dir, server_type + '.ring.gz')
        with closing(gzip.GzipFile(ring_path, 'wb')) as f:
            ring_data = create_replicated_ring_data(port, *devices)
            pickle.dump(ring_data, f)
        logger = utils.debug_logger(server_type)
        app = controller_class(conf, logger=logger)
        server = eventlet.spawn(eventlet.wsgi.server, socket, app, logger)
        servers.append(server)
    return servers


@utils.patch_policies([
    storage_policy.ECStoragePolicy(0, 'ec', ec_type='jerasure_rs_vand',
                                   ec_ndata=2, ec_nparity=1,
                                   ec_segment_size=4096)
])
class TestKineticECReplicator(utils.KineticSwiftTestCase):

    PORTS = (9010, 9020, 9030, 9040)

    def setUp(self):
        self.servers = []
        super(TestKineticECReplicator, self).setUp()
        self.servers.extend(create_ac_servers(self.test_dir))
        # create object server & rings
        obj_socket = eventlet.listen(('127.0.0.1', 0))
        obj_port = obj_socket.getsockname()[1]
        create_ec_rings(self.test_dir, obj_port, *self.ports)
        recon_cache_path = os.path.join(self.test_dir, 'recon_cache')
        os.makedirs(recon_cache_path)
        internal_client_path = os.path.join(
            self.test_dir, 'internal-client.conf')
        conf = {
            'swift_dir': self.test_dir,
            'mount_check': False,
            'recon_cache_path': recon_cache_path,
            'internal_client_conf_path': internal_client_path,
        }
        server.install_kinetic_diskfile()
        self.app = server.ObjectController(
            conf, logger=utils.debug_logger('object'))
        self.servers.append(eventlet.spawn(
            eventlet.wsgi.server, obj_socket, self.app, self.app.logger))
        # setup replicator daemon
        parser = ConfigParser()
        parser.readfp(StringIO(ic_conf_body))
        parser.set('DEFAULT', 'swift_dir', self.test_dir)
        parser.set('DEFAULT', 'account_autocreate', 'true')
        parser.set('DEFAULT', 'memcache_servers', '127.0.0.1:666')
        with open(internal_client_path, 'w') as f:
            parser.write(f)
        self.daemon = replicator.KineticReplicator(conf)
        # force ring reload
        for policy in server.diskfile.POLICIES:
            policy.object_ring = None
            self.daemon.load_object_ring(policy)
        self.logger = self.daemon.logger = \
            utils.debug_logger('test-kinetic-replicator')
        self.policy = random.choice([
            p for p in storage_policy.POLICIES
            if p.policy_type == storage_policy.EC_POLICY])
        # give the servers a chance to start
        timeout = time.time() + 30
        i = 0
        while time.time() < timeout:
            try:
                self.daemon.swift.create_container('a', 'c')
            except Exception:
                self.logger.debug('failed to create account attempt #%d' % i)
            else:
                break
            i += 1
        else:
            self.tearDown()
            self.fail('failed to create a container after %s attempts' % i)

    def tearDown(self):
        for coro in self.servers:
            coro.kill()
        super(TestKineticECReplicator, self).tearDown()

    def find_frags(self):
        frags = defaultdict(list)
        for port in self.ports:
            conn = self.client_map[port]
            resp = conn.getKeyRange('objects.', 'objects/')
            for key in resp.wait():
                key_info = replicator.split_key(key)
                frags[port].append(key_info['frag_index'])
        return frags

    def test_ec_object(self):
        test_body = ('a' * 4096 * 3)[:-438]
        fobj = StringIO(test_body)
        self.daemon.swift.upload_object(fobj, 'a', 'c', 'o')
        frags = self.find_frags()
        found_frag_indexes = set(itertools.chain(*frags.values()))
        self.assertEqual(len(found_frag_indexes), 3)
        status, headers, body_iter = self.daemon.swift.get_object(
            'a', 'c', 'o', {})
        self.assertEqual(status, 200)
        check_body = ''.join(body_iter)
        self.assertEqual(len(check_body), len(test_body))
        self.assertEqual(check_body, test_body)

    def test_ec_handoff(self):
        # fail one of the primary disks for the object
        part, nodes = self.policy.object_ring.get_nodes('a', 'c', 'o')
        bad_disk = int(random.choice(nodes)['device'].split(':', 1)[-1])
        self.stop_simulator(bad_disk)
        # upload an object
        self.daemon.swift.upload_object(StringIO('asdf'), 'a', 'c', 'o')
        # restart the simulator
        self.start_simulator(bad_disk)
        # verify it's on a handoff
        handoff_frags = self.find_frags()
        self.assertEqual(len(set(itertools.chain(
            *handoff_frags.values()))), 3)
        self.assertTrue(bad_disk not in handoff_frags)
        # run rebuild
        self.daemon.run_once()
        # verify it's back on the primary
        fixed_frags = self.find_frags()
        self.assertEqual(len(set(itertools.chain(
            *fixed_frags.values()))), 3)
        self.assertTrue(bad_disk in fixed_frags)

    def test_ec_rebuild(self):
        # upload an object
        self.daemon.swift.upload_object(StringIO('asdf'), 'a', 'c', 'o')
        # sanity, all frags on different nodes
        frags = self.find_frags()
        self.assertEqual(len(frags), 3)
        # pick one of the primaries to blow up
        part, nodes = self.policy.object_ring.get_nodes('a', 'c', 'o')
        bad_disk = int(random.choice(nodes)['device'].split(':', 1)[-1])
        conn = self.client_map[bad_disk]
        # delete the head key
        resp = conn.getKeyRange('objects.', 'objects/')
        for key in resp.wait():
            conn.delete(key)
        # sanity
        frags = self.find_frags()
        self.assertEqual(len(set(itertools.chain(
            *frags.values()))), 2)
        # run rebuild
        for i in range(3):
            self.daemon.run_once()
        # verify it's back on the primary
        fixed_frags = self.find_frags()
        self.assertEqual(len(set(itertools.chain(
            *fixed_frags.values()))), 3)
        self.assertTrue(bad_disk in fixed_frags)


if __name__ == "__main__":
    utils.unittest.main()
