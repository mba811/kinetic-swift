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

import hashlib
import time
import random
import mock
import msgpack

from swift.common.storage_policy import POLICIES

from kinetic_swift.obj import auditor, server

from utils import (KineticSwiftTestCase, debug_logger)


class TestKineticObjectAuditor(KineticSwiftTestCase):

    def setUp(self):
        super(TestKineticObjectAuditor, self).setUp()
        self.conf = {
            'unlink_wait': 'true',
        }
        self.auditor = auditor.KineticAuditor(self.conf)
        self.logger = debug_logger()
        self.auditor.logger = self.logger
        self.port = self.ports[0]
        self.client = self.client_map[self.port]
        self.device = 'localhost:%s' % self.port
        self.policy = random.choice(list(POLICIES))

    def test_put_and_audit(self):
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'), self.policy)
        body = 'awesome'
        with df.create() as writer:
            writer.write(body)
            etag = hashlib.md5(body).hexdigest()
            writer.put({'X-Timestamp': time.time(),
                        'ETag': etag,
                        'Content-Length': len(body)})

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'success': 1,
        })

        # check read object
        with df.open() as reader:
            metadata = reader.get_metadata()
            self.assertEqual(body, ''.join(reader))
        expected = {
            'ETag': etag,
            'Content-Length': len(body),
        }
        for k, v in expected.items():
            self.assertEqual(v, metadata[k])

    def test_delete_and_audit(self):
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'), self.policy)
        body = 'awesome'
        with df.create() as writer:
            writer.write(body)
            etag = hashlib.md5(body).hexdigest()
            writer.put({'X-Timestamp': time.time(),
                        'ETag': etag,
                        'Content-Length': len(body)})

        df.delete(time.time())

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'success': 1,
        })

        self.assertRaises(auditor.DiskFileDeleted, df.open)
        self.assertTrue('does not exist' in
                        self.auditor.logger.get_lines_for_level('debug')[0])

    def test_missing_chunk_key(self):
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'), self.policy)
        body = 'awesome'
        hash_ = hashlib.md5()
        with df.create() as writer:
            writer.write(body)
            hash_.update(body)
            writer.put({'X-Timestamp': time.time(),
                        'ETag': hash_.hexdigest(),
                        'Content-Length': len(body)})

        for key in self.client.getKeyRange('chunks', 'chunks/').wait():
            self.client.delete(key).wait()

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'failures': 1,
        })

        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines))
        for line in warning_lines:
            self.assert_('size' in line)

        # check quarantine
        try:
            df.open()
        except server.diskfile.DiskFileNotExist:
            pass
        else:
            self.fail('Did not raise deleted!')
        finally:
            df.close()

    def test_invalid_chunk(self):
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'), self.policy)
        body = 'awesome'
        hash_ = hashlib.md5()
        with df.create() as writer:
            writer.write(body)
            hash_.update(body)
            writer.put({'X-Timestamp': time.time(),
                        'ETag': hash_.hexdigest(),
                        'Content-Length': str(len(body))})

        for key in self.client.getKeyRange('chunks', 'chunks/').wait():
            self.client.put(key, 'a' * len(body)).wait()

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'failures': 1,
        })

        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines))
        for line in warning_lines:
            self.assert_('etag' in line)

        try:
            df.open()
        except server.diskfile.DiskFileNotExist:
            pass
        else:
            self.fail('Did not raise deleted!')
        finally:
            df.close()

    def test_quarantine(self):
        num_chunks = 8
        chunk_size = 100
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'), self.policy,
                                           disk_chunk_size=chunk_size)
        chunk = 'a' * chunk_size
        hash_ = hashlib.md5()
        put_timestamp = server.diskfile.Timestamp(time.time())
        with df.create() as writer:
            for i in range(num_chunks):
                writer.write(chunk)
                hash_.update(chunk)
            writer.put({'X-Timestamp': put_timestamp.internal,
                        'ETag': hash_.hexdigest(),
                        'Content-Length': str(num_chunks * chunk_size)})

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'success': 1,
        })

        # check quarantine keys
        with df.open():
            nonce = df._nonce
            df.quarantine()

        start_key = 'quarantine'
        end_key = start_key + '/'
        keys = [k for k in self.client.getKeyRange(start_key, end_key).wait()
                if nonce in k]
        self.assertEqual(len(keys), num_chunks + 1)

        chunk_keys = [k for k in keys if 'chunk' in k]
        etag = hashlib.md5()
        size = 0
        for k in sorted(chunk_keys):
            chunk = self.client.get(k).wait().value
            size += len(chunk)
            etag.update(chunk)

        self.assertEqual(etag.hexdigest(), hash_.hexdigest())
        self.assertEqual(size, num_chunks * chunk_size)

        head_key = [k for k in keys if 'object' in k][0]
        metadata = msgpack.unpackb(self.client.get(head_key).wait().value)
        expected = {
            'Content-Length': str(num_chunks * chunk_size),
            'ETag': etag.hexdigest(),
            'X-Kinetic-Chunk-Count': num_chunks,
            'X-Kinetic-Chunk-Nonce': nonce,
            'X-Timestamp': put_timestamp.internal,
            'name': '/a/c/%s' % self.buildKey('o'),
        }
        self.assertEqual(metadata, expected)
