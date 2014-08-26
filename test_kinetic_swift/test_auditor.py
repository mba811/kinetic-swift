import hashlib
import time
import random
import mock

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
                                           self.buildKey('o'),
                                           policy_idx=int(self.policy))
        body = 'awesome'
        with df.create() as writer:
            writer.write(body)
            etag = hashlib.md5(body).hexdigest()
            writer.put({'X-Timestamp': time.time(),
                        'Etag': etag,
                        'Content-Length': len(body)})

        with mock.patch('time.sleep', lambda x: None):
            self.auditor.run_once(devices=self.device)

        self.auditor.logger.logger.lines_dict
        self.assertEqual(self.auditor.stats, {
            'found_objects': 1,
            'success': 1,
        })

        # check read object
        with df.open() as reader:
            metadata = reader.get_metadata()
            self.assertEqual(body, ''.join(reader))
        expected = {
            'Etag': etag,
            'Content-Length': len(body),
        }
        for k, v in expected.items():
            self.assertEqual(v, metadata[k])

    def test_missing_chunk_key(self):
        df = self.auditor.mgr.get_diskfile(self.device, '0', 'a', 'c',
                                           self.buildKey('o'),
                                           policy_idx=int(self.policy))
        body = 'awesome'
        hash_ = hashlib.md5()
        with df.create() as writer:
            writer.write(body)
            hash_.update(body)
            writer.put({'X-Timestamp': time.time(),
                        'Etag': hash_.hexdigest(),
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
                                           self.buildKey('o'),
                                           policy_idx=int(self.policy))
        body = 'awesome'
        hash_ = hashlib.md5()
        with df.create() as writer:
            writer.write(body)
            hash_.update(body)
            writer.put({'X-Timestamp': time.time(),
                        'Etag': hash_.hexdigest(),
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
