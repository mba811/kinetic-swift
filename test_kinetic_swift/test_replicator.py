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

from contextlib import closing
import cPickle as pickle
import gzip
import itertools
import os
import time
import random
import unittest
import uuid

from swift.common import ring, storage_policy, utils as swift_utils

from kinetic_swift.obj import replicator, server

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
        key = 'objects.%s.%s.%s' % (hash_, t.internal, nonce)
        expected = {
            'hashpath': hash_,
            'nonce': str(nonce),
            'policy': storage_policy.POLICIES.legacy,
            'timestamp': t.internal,
        }
        self.assertEqual(replicator.split_key(key), expected)

    def test_multiple_polices(self):
        hash_ = swift_utils.hash_path('a', 'c', 'o')
        t = swift_utils.Timestamp(time.time())
        nonce = uuid.uuid4()
        with utils.patch_policies(with_ec_default=True):
            for p in storage_policy.POLICIES:
                object_prefix = storage_policy.get_policy_string('objects', p)
                key = '%(prefix)s.%(hash)s.%(timestamp)s.%(nonce)s' % {
                    'prefix': object_prefix,
                    'hash': hash_,
                    'timestamp': t.internal,
                    'nonce': nonce,
                }
                expected = {
                    'hashpath': hash_,
                    'policy': p,
                    'nonce': str(nonce),
                    'timestamp': t.internal,
                }
                self.assertEqual(replicator.split_key(key), expected)


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
        }
        self.daemon = replicator.KineticReplicator(conf)
        # force ring reload
        for policy in server.diskfile.POLICIES:
            policy.object_ring = None
            self.daemon.load_object_ring(policy)
        self.logger = self.daemon.logger = \
            utils.debug_logger('test-kinetic-replicator')
        self._df_router = server.diskfile.DiskFileRouter({}, self.logger)
        self.policy = random.choice(list(server.diskfile.POLICIES))
        self.mgr = self._df_router[self.policy]

    def put_object(self, device, object_name, body='', timestamp=None,
                   policy=None):
        policy = policy or self.policy
        df = self.mgr.get_diskfile(device, '0', 'a', 'c', object_name,
                                   policy=policy)
        metadata = {'X-Timestamp': timestamp or time.time()}
        with df.create() as writer:
            writer.write(body)
            writer.put(metadata)
        return metadata, body

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

    @utils.patch_policies(with_ec_default=True)
    def test_do_not_replicate_ec_policy(self):
        for policy in server.diskfile.POLICIES:
            if policy.policy_type != storage_policy.EC_POLICY:
                policy.object_ring = None
                self.daemon.load_object_ring(policy)
        self.daemon.run_once()
        found_storage_policies = set()
        for msg in self.daemon.logger.get_lines_for_level('debug'):
            if 'begin replication' not in msg.lower():
                continue
            for policy in storage_policy.POLICIES:
                if str(policy) in msg:
                    found_storage_policies.add(policy)
        self.assertEqual(found_storage_policies, set([
            p for p in storage_policy.POLICIES
            if p.policy_type != storage_policy.EC_POLICY
        ]))

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
        self.daemon._replicate(source_device)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        source_resp = source_client.getKeyRange('chunks.', 'objects/')
        target_resp = target_client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        target_keys = target_resp.wait()
        self.assertEquals(source_keys, target_keys)
        self.assertEqual(len(self.daemon._conn_pool),
                         self.policy.object_ring.replica_count)

    def test_replicate_to_right_servers(self):
        source_device = '127.0.0.1:%s' % self.ports[0]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        other_port = self.ports[2]
        other_device = '127.0.0.1:%s' % other_port

        expected = self.put_object(source_device, 'obj1')
        self.daemon._replicate(source_device)
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
        self.daemon._replicate(source_device)
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
        self.daemon._replicate(source_device)
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
        self.daemon._replicate(other_device)
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
        self.daemon._replicate(other_device)
        result = self.get_object(source_device, 'obj1')
        self.assertEquals(expected, result)
        result = self.get_object(target_device, 'obj1')
        self.assertEquals(expected, result)
        # and now it's gone from handoff
        self.assertRaises(server.diskfile.DiskFileNotExist, self.get_object,
                          other_device, 'obj1')


class TestKineticCopyReplicator(TestKineticReplicator):

    REPLICATION_MODE = 'copy'


if __name__ == "__main__":
    utils.unittest.main()
