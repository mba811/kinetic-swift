import time
import unittest

from kinetic import greenclient

from kinetic_swift.obj import server

from utils import KineticSwiftTestCase, FakeLogger


class TestDiskFile(KineticSwiftTestCase):

    def setUp(self):
        super(TestDiskFile, self).setUp()
        self.port = self.ports[0]
        self.device = 'localhost:%s' % self.port
        self.client = self.client_map[self.port]

    def test_create(self):
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger())
        self.assert_(isinstance(df.conn, greenclient.GreenClient))

    def test_open(self):
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger())
        with df.open():
            metadata = df.get_metadata()
        self.assertEquals(metadata, {})

    def test_put(self):
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger())
        with df.create() as writer:
            writer.write('awesome')
            writer.put({'X-Timestamp': time.time()})

    def test_put_and_get(self):
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger())
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

    def test_multi_chunk_put_and_get(self):
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger(),
                             disk_chunk_size=10)
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

        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger(),
                             disk_chunk_size=disk_chunk_size)
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
        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger(),
                             disk_chunk_size=10)
        req_timestamp = time.time()
        with df.create() as writer:
            chunk = '\x00' * 10
            for i in range(3):
                writer.write(chunk)
            writer.put({'X-Timestamp': req_timestamp})

        req_timestamp += 1
        df.delete(req_timestamp)

        with df.open() as reader:
            metadata = reader.get_metadata()
            is_deleted = reader.is_deleted()

        self.assertTrue(metadata['deleted'])
        self.assertTrue(is_deleted)

    def test_overwrite(self):
        num_chunks = 3
        disk_chunk_size = 10
        disk_chunk_count = num_chunks - 1

        df = server.DiskFile('/srv/node', self.device, '0', 'a', 'c',
                             self.buildKey('o'), FakeLogger(),
                             disk_chunk_size=10)
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
        start_key = 'objects.%s' % df.hashpath
        end_key = 'objects.%s/' % df.hashpath
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
