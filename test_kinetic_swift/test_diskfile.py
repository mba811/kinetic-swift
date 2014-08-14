import time
import unittest
import random

from swift.common.utils import Timestamp

from kinetic_swift.client import KineticSwiftClient
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
        self.mgr.unlink_wait = True
        self.policy = random.choice(list(server.diskfile.POLICIES))

    def test_manager_config(self):
        conf = {
            'connect_retry': '6',
            'connect_timeout': '10',
            'response_timeout': '90',
            'write_depth': '2',
            'delete_depth': '4',
            'disk_chunk_size': '%s' % 2 ** 20,
        }
        mgr = server.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.connect_retry, 6)
        df = mgr.get_diskfile(self.device, '0', 'a', 'c', self.buildKey('o'),
                              policy_idx=int(self.policy))
        self.assertEqual(df.conn.conn.connect_timeout, 10)
        self.assertEqual(df.conn.response_timeout, 90)
        self.assertEqual(df.write_depth, 2)
        self.assertEqual(df.delete_depth, 4)
        self.assertEqual(df.disk_chunk_size, 2 ** 20)

    def test_config_sync_options(self):
        expectations = {
            'default': -1,
            'writethrough': 1,
            'writeback': 2,
            'flush': 3,
        }
        for option, expected in expectations.items():
            conf = {'synchronization': option}
            mgr = server.DiskFileManager(conf, self.logger)
            self.assertEqual(mgr.synchronization, expected)

    def test_create(self):
        df = self.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                   self.buildKey('o'),
                                   policy_idx=int(self.policy))
        self.assert_(isinstance(df.conn, KineticSwiftClient))

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
            'X-Kinetic-Chunk-Count': 1,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v,
                             'expected %r for metadatakey %r got %r' % (
                                 v, k, metadata[k]))

    def test_submit_write_all_sync_options(self):
        for sync_option in ('flush', 'writeback', 'writethrough', 'default'):
            conf = {'synchronization': sync_option}
            mgr = server.DiskFileManager(conf, self.logger)
            df = mgr.get_diskfile(self.device, '0', 'a', 'c',
                                  self.buildKey('o'),
                                  policy_idx=int(self.policy))
            options = {}

            def capture_args(*args, **kwargs):
                options.update(kwargs)
            df.conn.put = capture_args
            with df.create():
                key = self.buildKey('submit_%s' % sync_option)
                df._submit_write(key, 'blob', final=False)
                # flush option does writeback unless final
                if sync_option == 'flush':
                    self.assertEqual(options['synchronization'],
                                     server.SYNC_OPTION_MAP['writeback'])
                else:
                    self.assertEqual(options['synchronization'],
                                     df.synchronization)
                # final write always matches sync option
                key = self.buildKey('submit_final_%s' % sync_option)
                df._submit_write(key, 'blob', final=True)
                self.assertEqual(options['synchronization'],
                                 df.synchronization)

    def test_put_all_sync_options(self):
        expected_body = 'a' * 100
        conf = {
            'disk_chunk_size': 10,
        }
        for sync_option in ('flush', 'writeback', 'writethrough', 'default'):
            conf['synchronization'] = sync_option
            mgr = server.DiskFileManager(conf, self.logger)
            mgr.unlink_wait = True
            df = mgr.get_diskfile(self.device, '0', 'a', 'c',
                                  self.buildKey('o'),
                                  policy_idx=int(self.policy))
            req_timestamp = time.time()
            with df.create() as writer:
                writer.write(expected_body)
                writer.put({'X-Timestamp': req_timestamp})

            with df.open() as reader:
                metadata = reader.get_metadata()
                body = ''.join(reader)

            self.assertEquals(body, expected_body)
            expected = {
                'X-Timestamp': req_timestamp,
                'X-Kinetic-Chunk-Count': 10,
            }
            for k, v in expected.items():
                self.assertEqual(metadata[k], v,
                                 'expected %r for metadatakey %r got %r' % (
                                     v, k, metadata[k]))

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
            'X-Kinetic-Chunk-Count': 3,
        }
        for k, v in expected.items():
            self.assertEqual(metadata[k], v)

    def test_multi_chunk_put_and_get_with_buffer_offset(self):
        disk_chunk_size = 10
        write_chunk_size = 6
        write_chunk_count = 7
        object_size = write_chunk_size * write_chunk_count
        # int(math.ceil(1.0 * object_size / disk_chunk_size))
        q, r = divmod(object_size, disk_chunk_size)
        disk_chunk_count = q if not r else q + 1

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

        # check object keys
        storage_policy = server.diskfile.get_data_dir(int(self.policy))
        start_key = '%s.%s' % (storage_policy, df.hashpath)
        end_key = '%s.%s/' % (storage_policy, df.hashpath)
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))  # the tombstone!
        for key in keys:
            expected = start_key + '.%s.ts' % Timestamp(req_timestamp).internal
            self.assert_(key.startswith(expected))

        # check chunk keys
        start_key = 'chunks.%s' % df.hashpath
        end_key = 'chunks.%s/' % df.hashpath
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(0, len(keys))

    def test_overwrite(self):
        num_chunks = 3
        disk_chunk_size = 10
        disk_chunk_count = num_chunks

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
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(1, len(keys))

        # check chunk keys
        start_key = 'chunks.%s' % df.hashpath
        end_key = 'chunks.%s/' % df.hashpath
        keys = self.client.getKeyRange(start_key, end_key).wait()
        self.assertEqual(disk_chunk_count, len(keys))


if __name__ == "__main__":
    unittest.main()
