import time
import unittest
import random

from kinetic import greenclient

from kinetic_swift.obj import server

from utils import KineticSwiftTestCase, debug_logger


class TestDiskFile(KineticSwiftTestCase):

    def setUp(self):
        super(TestDiskFile, self).setUp()
        self.port = self.ports[0]
        self.device = 'localhost:%s' % self.port
        self.client = self.client_map[self.port]
        self.logger = debug_logger('test-kinetic')
        self.mgr = server.DiskFileManager({}, self.logger)
        self.policy = random.choice(list(server.diskfile.POLICIES))

    def test_manager_config(self):
        conf = {
            'connect_timeout': 10,
            'write_depth': 2,
            'disk_chunk_size': 2 ** 20,
        }
        mgr = server.DiskFileManager(conf, self.logger)
        df = mgr.get_diskfile(self.device, '0', 'a', 'c', self.buildKey('o'),
                              policy_idx=int(self.policy))
        self.assertEqual(df.conn.connect_timeout, 10)
        self.assertEqual(df.write_depth, 2)
        self.assertEqual(df.disk_chunk_size, 2 ** 20)

    def test_create(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   policy_idx=int(self.policy))
        self.assert_(isinstance(df.conn, greenclient.GreenClient))

    def test_put(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   policy_idx=int(self.policy))
        with df.create() as writer:
            writer.write('awesome')
            writer.put({'X-Timestamp': time.time()})

    def test_put_and_get(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   policy_idx=int(self.policy))
        req_timestamp = time.time()
        with df.create() as writer:
            writer.write('awesome')
            writer.put({'X-Timestamp': req_timestamp})

        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)

        self.assertEquals(body, 'awesome')
        expected = {
            'X-Timestamp': req_timestamp,
            'X-Kinetic-Chunk-Count': 0,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v)

    def test_get_not_found(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   policy_idx=int(self.policy))
        try:
            df.open()
        except server.diskfile.DiskFileNotExist:
            pass
        else:
            self.fail('Did not raise deleted!')
        finally:
            df.close()

    def test_multi_chunk_put_and_get(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'), disk_chunk_size=10,
                                   policy_idx=int(self.policy))
        req_timestamp = time.time()
        with df.create() as writer:
            chunk = '\x00' * 10
            for i in range(3):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)

        self.assertEquals(body, '\x00' * 30)
        expected = {
            'X-Timestamp': req_timestamp,
            'X-Kinetic-Chunk-Count': 2,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v)

    def test_multi_chunk_put_and_get_with_buffer_offset(self):
        disk_chunk_size = 10
        write_chunk_size = 6
        write_chunk_count = 7
        object_size = write_chunk_size * write_chunk_count
        # int(math.ceil(1.0 * object_size / disk_chunk_size) - 1)
        q, r = divmod(object_size, disk_chunk_size)
        disk_chunk_count = q if r else q - 1

        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   disk_chunk_size=disk_chunk_size,
                                   policy_idx=int(self.policy))
        req_timestamp = time.time()
        with df.create() as writer:
            chunk = '\x00' * write_chunk_size
            for i in range(write_chunk_count):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)

        self.assertEquals(len(body), object_size)
        self.assertEquals(body, '\x00' * object_size)
        expected = {
            'X-Timestamp': req_timestamp,
            'X-Kinetic-Chunk-Count': disk_chunk_count,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v)

    def test_write_and_delete(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'), disk_chunk_size=10,
                                   policy_idx=int(self.policy))
        req_timestamp = time.time()
        with df.create() as writer:
            chunk = '\x00' * 10
            for i in range(3):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        req_timestamp += 1
        df.delete(req_timestamp)

        try:
            df.open()
        except server.diskfile.DiskFileDeleted as e:
            self.assertEqual(e.timestamp, req_timestamp)
        else:
            self.fail('Did not raise deleted!')
        finally:
            df.close()

    def test_overwrite(self):
        num_chunks = 3
        disk_chunk_size = 10
        disk_chunk_count = num_chunks - 1

        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'), disk_chunk_size=10,
                                   policy_idx=int(self.policy))
        req_timestamp = time.time()
        with df.create() as writer:
            chunk = '\x00' * disk_chunk_size
            for i in range(num_chunks + 1):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        req_timestamp += 1

        with df.create() as writer:
            chunk = '\x01' * disk_chunk_size
            for i in range(num_chunks):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        with df.open() as reader:
            metadata = reader.get_metadata()
            body = ''.join(reader)

        expected = {
            'X-Timestamp': req_timestamp,
            'X-Kinetic-Chunk-Count': disk_chunk_count,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v)
        self.assertEquals(body, '\x01' * (disk_chunk_size * num_chunks))

        # check object keys

        storage_policy = server.diskfile.get_data_dir(int(self.policy))
        start_key = '%s.%s' % (storage_policy, df.hashpath)
        end_key = '%s.%s/' % (storage_policy, df.hashpath)
        with self.client:
            keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))

        # check chunk keys
        start_key = 'chunks.%s' % df.hashpath
        end_key = 'chunks.%s/' % df.hashpath
        with self.client:
            keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(disk_chunk_count, len(keys))


if __name__ == "__main__":
    unittest.main()
