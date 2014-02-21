from contextlib import closing
import cPickle as pickle
import gzip
import os
import shutil
import tempfile
import time

from swift.common import ring

from kinetic_swift.obj import replicator, server

import utils


def create_ring(data_dir, *ports):
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

    object_ring_path = os.path.join(data_dir, 'object.ring.gz')
    with closing(gzip.GzipFile(object_ring_path, 'wb')) as f:
        ring_data = ring.RingData(replica2part2device, devices, 30)
        pickle.dump(ring_data, f)


class TestKineticReplicator(utils.KineticSwiftTestCase):

    REPLICATION_MODE = 'push'
    PORTS = (9010, 9020, 9030)

    def setUp(self):
        super(TestKineticReplicator, self).setUp()
        create_ring(self.test_dir, *self.ports)
        conf = {
            'swift_dir': self.test_dir,
            'kinetic_replication_mode': self.REPLICATION_MODE,
        }
        self.daemon = replicator.KineticReplicator(conf)

    def put_object(self, device, object_name, body='', timestamp=None):
        df = server.DiskFile('/srv/node', device, '0', 'a', 'c',
                             object_name, utils.FakeLogger())
        metadata = {'X-Timestamp': timestamp or time.time()}
        with df.create() as writer:
            writer.write(body)
            writer.put(metadata)
        return metadata, body

    def get_object(self, device, object_name):
        df = server.DiskFile('/srv/node', device, '0', 'a', 'c',
                             object_name, utils.FakeLogger())
        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)
        return metadata, body

    def test_setup(self):
        self.assertEqual(self.daemon.swift_dir, self.test_dir)
        expected_path = os.path.join(self.test_dir, 'object.ring.gz')
        self.assertEqual(self.daemon.object_ring.serialized_path, expected_path)
        self.assertEquals(len(self.daemon.object_ring.devs), len(self.ports))
        for client in self.client_map.values():
            with client:
                resp = client.getKeyRange('chunks.', 'objects/')
            self.assertEquals(resp.wait(), [])

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
        with source_client as client:
            source_resp = client.getKeyRange('chunks.', 'objects/')
        with target_client as client:
            target_resp = client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        target_keys = target_resp.wait()
        self.assertEquals(source_keys, target_keys)

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
        empty = self.get_object(other_device, 'obj1')
        expected = ({}, '')
        self.assertEqual(empty, expected)

    def test_replicate_random_chunks(self):
        _part, devices = self.daemon.object_ring.get_nodes('a', 'c', 'random_object')
        self.assertEquals(2, len(devices))
        source_device, target_device = [d['device'] for d in devices]
        object_size = 2 ** 20
        body = '\x00' * object_size
        expected = self.put_object(source_device, 'random_object', body=body)
        self.daemon._replicate(source_device)
        result = self.get_object(target_device, 'random_object')
        self.assertEquals(result, expected)

    def test_nounce_reconciliation(self):
        source_port = self.ports[0]
        source_device = '127.0.0.1:%s' % source_port
        source_client = self.client_map[source_port]
        target_port = self.ports[1]
        target_device = '127.0.0.1:%s' % target_port
        target_client = self.client_map[target_port]
        # two requests with identical timestamp and different nounce
        req_timestamp = time.time()
        self.put_object(source_device, 'obj1', timestamp=req_timestamp)
        self.put_object(target_device, 'obj1', timestamp=req_timestamp)
        source_meta, _body = self.get_object(source_device, 'obj1')
        target_meta, _body = self.get_object(target_device, 'obj1')
        self.assertNotEqual(source_meta, target_meta)
        source_nounce = source_meta.pop('X-Kinetic-Chunk-Nounce')
        target_nounce = target_meta.pop('X-Kinetic-Chunk-Nounce')
        self.assertNotEqual(source_nounce, target_nounce)
        self.assertEqual(source_meta, target_meta)
        # peek at keys
        with source_client as client:
            source_resp = client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        with target_client as client:
            target_resp = client.getKeyRange('chunks.', 'objects/')
        target_keys = target_resp.wait()
        self.assertEqual(len(source_keys), len(target_keys))
        for source_key, target_key in zip(source_keys, target_keys):
            source_hash, source_nounce = replicator.split_key(source_key)
            target_hash, target_nounce = replicator.split_key(target_key)
            self.assertEqual(source_hash, target_hash)
            self.assertNotEqual(source_nounce, target_nounce)
        original_key_count = len(source_keys)
        # perform replication, should more or less no-op
        self.daemon._replicate(source_device)
        with source_client as client:
            source_resp = client.getKeyRange('chunks.', 'objects/')
        source_keys = source_resp.wait()
        with target_client as client:
            target_resp = client.getKeyRange('chunks.', 'objects/')
        target_keys = target_resp.wait()
        self.assertEqual(len(source_keys), len(target_keys))
        for source_key, target_key in zip(source_keys, target_keys):
            source_hash, source_nounce = replicator.split_key(source_key)
            target_hash, target_nounce = replicator.split_key(target_key)
            self.assertEqual(source_hash, target_hash)
            self.assertNotEqual(source_nounce, target_nounce)
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
        empty = self.get_object(other_device, 'obj1')
        expected = ({}, '')
        self.assertEqual(empty, expected)


class TestKineticCopyReplicator(TestKineticReplicator):

    REPLICATION_MODE = 'copy'

    def test_setup(self):
        self.assertEqual(self.daemon.replication_mode, 'copy')


if __name__ == "__main__":
    utils.unittest.main()
