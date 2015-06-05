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

import os
import logging
from contextlib import contextmanager
from collections import deque
from uuid import uuid4
from eventlet import sleep, Timeout, spawn_n
import re
import time

import msgpack
from swift.obj import diskfile, server
from swift.common.storage_policy import (POLICIES, split_policy_string,
                                         PolicyError)

from kinetic_swift.client import KineticSwiftClient

from kinetic.common import Synchronization


DEFAULT_DEPTH = 2


SYNC_OPTION_MAP = {
    'default': None,
    'writethrough': Synchronization.WRITETHROUGH,
    'writeback': Synchronization.WRITEBACK,
    'flush': Synchronization.FLUSH,
}


def chunk_key(hashpath, nonce, index=None):
    if index is None:
        # for use with getKeyRange
        key = 'chunks.%s.%s/' % (hashpath, nonce)
    else:
        key = 'chunks.%s.%s.%0.32d' % (hashpath, nonce, index)
    return key


def object_key(policy, hashpath, timestamp='',
               extension='.data', nonce=''):
    storage_policy = diskfile.get_data_dir(policy)
    if timestamp:
        return '%s.%s.%s%s.%s' % (storage_policy, hashpath, timestamp,
                                  extension, nonce)
    else:
        # for use with getPrevious
        return '%s.%s/' % (storage_policy, hashpath)


def async_key(policy, hashpath, timestamp):
    async_policy = diskfile.get_async_dir(policy)
    return '%s.%s.%s' % (async_policy, hashpath, timestamp)


def get_nonce(key):
    return key.rsplit('.', 1)[-1]


class DiskFileReader(diskfile.DiskFileReader):

    def __init__(self, diskfile):
        self._use_splice = False
        self.diskfile = diskfile
        self._suppress_file_closing = False

    def app_iter_range(self, start, stop):
        r = 0
        if start or start == 0:
            q, r = divmod(start, self.diskfile.disk_chunk_size)
            self.diskfile.chunk_id = q
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            for chunk in self:
                if length is not None:
                    length -= len(chunk) - r
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[r:length]
                        break
                yield chunk[r:]
                r = 0
        finally:
            if not self._suppress_file_closing:
                self.close()

    def __iter__(self):
        return iter(self.diskfile)

    def close(self):
        return self.diskfile.close()


class DiskFile(diskfile.DiskFile):

    reader_cls = DiskFileReader

    def __init__(self, mgr, host, port, *args, **kwargs):
        self.unlink_wait = kwargs.pop('unlink_wait', False)
        device_path = ''
        self.disk_chunk_size = kwargs.pop('disk_chunk_size',
                                          mgr.disk_chunk_size)
        # this is normally setup in DiskFileWriter, but we do it here
        self._extension = '.data'
        # this is to neuter the context manager close in GET
        self._took_reader = False
        super(DiskFile, self).__init__(mgr, device_path, *args, **kwargs)
        self.hashpath = os.path.basename(self._datadir.rstrip('/'))
        self._buffer = bytearray()
        self._nonce = None
        self.chunk_id = 0
        self.upload_size = 0
        self.last_sync = 0
        # configurables
        self.write_depth = self._manager.write_depth
        self.read_depth = self._manager.read_depth
        self.delete_depth = self._manager.delete_depth
        self.synchronization = self._manager.synchronization
        self.conn = None
        self.conn = mgr.get_connection(host, port)
        self.logger = mgr.logger

    def object_key(self, *args, **kwargs):
        return object_key(self.policy, self.hashpath, *args, **kwargs)

    def _read(self):
        key = self.object_key()
        entry = self.conn.getPrevious(key).wait()
        if not entry or not entry.key.startswith(key[:-1]):
            self._metadata = {}  # mark object as "open"
            return
        self.data_file = '.ts.' not in entry.key
        blob = entry.value
        self._nonce = get_nonce(entry.key)
        self._metadata = msgpack.unpackb(blob)

    def open(self, **kwargs):
        self._read()
        if not self._metadata:
            raise diskfile.DiskFileNotExist()
        if self._metadata.get('deleted', False):
            raise diskfile.DiskFileDeleted(metadata=self._metadata)
        return self

    def reader(self, *args, **kwargs):
        self._took_reader = True
        return DiskFileReader(self)

    def close(self, **kwargs):
        self._metadata = None

    def __exit__(self, t, v, tb):
        if not self._took_reader:
            self.close()

    def keys(self):
        return [chunk_key(self.hashpath, self._nonce, i + 1) for i in
                range(self.chunk_id,
                      int(self._metadata['X-Kinetic-Chunk-Count']))]

    def __iter__(self):
        if not self._metadata:
            return
        pending = deque()
        for key in self.keys():
            while len(pending) >= self.read_depth:
                entry = pending.popleft().wait()
                yield str(entry.value) if entry else ''
            pending.append(self.conn.get(key))
        for resp in pending:
            entry = resp.wait()
            yield str(entry.value) if entry else ''

    @contextmanager
    def create(self, size=None):
        self._nonce = str(uuid4())
        self._chunk_id = 0
        try:
            self._pending_write = deque()
            yield self
        finally:
            self.close()

    def commit(self, timestamp):
        pass

    def write_metadata(self, metadata):
        raise NotImplementedError()

    def write(self, chunk):
        self._buffer.extend(chunk)
        self.upload_size += len(chunk)

        diff = self.upload_size - self.last_sync
        if diff >= self.disk_chunk_size:
            self._sync_buffer()
            self.last_sync = self.upload_size
        return self.upload_size

    def _submit_write(self, key, blob, final=True):
        if len(self._pending_write) >= self.write_depth:
            self._pending_write.popleft().wait()
        if self.synchronization == Synchronization.FLUSH and not final:
            synchronization = Synchronization.WRITEBACK
        else:
            synchronization = self.synchronization
        pending_resp = self.conn.put(key, blob, force=True,
                                     synchronization=synchronization)
        self._pending_write.append(pending_resp)

    def _sync_buffer(self):
        if self._buffer:
            # write out the chunk buffer!
            self._chunk_id += 1
            key = chunk_key(self.hashpath, self._nonce, self._chunk_id)
            self._submit_write(key, self._buffer[:self.disk_chunk_size],
                               final=False)
        self._buffer = self._buffer[self.disk_chunk_size:]

    def _wait_write(self):
        for resp in self._pending_write:
            resp.wait()

    def delete(self, timestamp):
        timestamp = diskfile.Timestamp(timestamp).internal

        with self.create() as deleter:
            deleter._extension = '.ts'
            deleter.put({'X-Timestamp': timestamp})

    def put(self, metadata):
        if self._extension == '.ts':
            metadata['deleted'] = True
        self._sync_buffer()
        while self._buffer:
            self._sync_buffer()
        # zero index, chunk-count is len
        metadata['X-Kinetic-Chunk-Count'] = self._chunk_id
        metadata['X-Kinetic-Chunk-Nonce'] = self._nonce
        metadata['name'] = self._name
        self._metadata = metadata
        blob = msgpack.packb(self._metadata)
        timestamp = diskfile.Timestamp(metadata['X-Timestamp'])
        key = self.object_key(timestamp.internal, self._extension,
                              self._nonce)
        self._submit_write(key, blob, final=True)
        self._wait_write()
        if self.unlink_wait:
            self._unlink_old(timestamp)
        else:
            spawn_n(self._unlink_old, timestamp)

    def _unlink_old(self, req_timestamp):
        start_key = self.object_key()[:-1]
        end_key = self.object_key(timestamp=req_timestamp.internal)
        resp = self.conn.getKeyRange(start_key, end_key, endKeyInclusive=False)
        head_keys = resp.wait()
        pending = deque()
        for head_key in head_keys:
            nonce = get_nonce(head_key)

            def key_gen():
                start_key = chunk_key(self.hashpath, nonce, 0)
                end_key = chunk_key(self.hashpath, nonce)
                resp = self.conn.getKeyRange(start_key, end_key,
                                             endKeyInclusive=False)
                chunk_keys = resp.wait()
                for key in chunk_keys:
                    yield key
                yield head_key

            for key in key_gen():
                while len(pending) >= self.delete_depth:
                    found = pending.popleft().wait()
                    if not found:
                        break
                pending.append(self.conn.delete(key, force=True))

        for resp in pending:
            resp.wait()

    def quarantine(self):
        timestamp = diskfile.Timestamp(self._metadata['X-Timestamp'])
        head_key = self.object_key(timestamp.internal, self._extension,
                                   self._nonce)
        keys = [head_key] + [
            chunk_key(self.hashpath, self._nonce, i + 1) for i in
            range(int(self._metadata['X-Kinetic-Chunk-Count']))]
        quarantine_prefix = 'quarantine.%s.' % diskfile.Timestamp(
            time.time()).internal
        for key in keys:
            resp = self.conn.rename(key, quarantine_prefix + key)
            resp.wait()

    def get_data_file_size(self):
        return self._metadata['Content-Length']


class DiskFileManager(diskfile.DiskFileManager):

    diskfile_cls = DiskFile

    def __init__(self, conf, logger):
        super(DiskFileManager, self).__init__(conf, logger)
        self.connect_timeout = int(conf.get('connect_timeout', 3))
        self.response_timeout = int(conf.get('response_timeout', 30))
        self.connect_retry = int(conf.get('connect_retry', 3))
        self.write_depth = int(conf.get('write_depth', DEFAULT_DEPTH))
        self.read_depth = int(conf.get('read_depth', DEFAULT_DEPTH))
        self.delete_depth = int(conf.get('delete_depth', DEFAULT_DEPTH))
        raw_sync_option = conf.get('synchronization', 'writeback').lower()
        try:
            self.synchronization = SYNC_OPTION_MAP[raw_sync_option]
        except KeyError:
            raise ValueError('Invalid synchronization option, choices are %r' %
                             SYNC_OPTION_MAP.keys())
        self.conn_pool = {}
        self.unlink_wait = \
            server.config_true_value(conf.get('unlink_wait', 'false'))

    def get_diskfile(self, device, partition, account, container, obj, policy,
                     **kwargs):
        host, port = device.split(':')
        return DiskFile(self, host, port, self.threadpools[device],
                        partition, account, container, obj, policy=policy,
                        unlink_wait=self.unlink_wait,
                        **kwargs)

    def get_diskfile_from_audit_location(self, device, head_key):
        host, port = device.split(':')
        policy_match = re.match('objects([-]?[0-9]?)\.', head_key)
        policy_string = policy_match.group(1)

        try:
            _, policy = split_policy_string(policy_string)
        except PolicyError:
            policy = POLICIES.legacy
        datadir = head_key.split('.', 3)[1]
        return DiskFile(self, host, port, self.threadpools[device], None,
                        policy=policy, _datadir=datadir,
                        unlink_wait=self.unlink_wait)

    def pickle_async_update(self, device, account, container, obj, data,
                            timestamp, policy_idx):
        host, port = device.split(':')
        hashpath = diskfile.hash_path(account, container, obj)
        key = async_key(policy_idx, hashpath, timestamp)
        blob = msgpack.packb(data)
        resp = self.get_connection(host, port).put(key, blob)
        resp.wait()
        self.logger.increment('async_pendings')

    def _new_connection(self, host, port, **kwargs):
        kwargs.setdefault('connect_timeout', self.connect_timeout)
        kwargs.setdefault('response_timeout', self.response_timeout)
        for i in range(1, self.connect_retry + 1):
            try:
                return KineticSwiftClient(self.logger, host, int(port),
                                          **kwargs)
            except Timeout:
                self.logger.warning('Drive %s:%s connect timeout #%d (%ds)' % (
                    host, port, i, self.connect_timeout))
            except Exception:
                self.logger.exception('Drive %s:%s connection error #%d' % (
                    host, port, i))
            if i < self.connect_retry:
                sleep(1)
        msg = 'Unable to connect to drive %s:%s after %s attempts' % (
            host, port, i)
        self.logger.error(msg)
        raise diskfile.DiskFileDeviceUnavailable()

    def get_connection(self, host, port, **kwargs):
        key = (host, port)
        conn = None
        try:
            conn = self.conn_pool[key]
        except KeyError:
            pass
        if conn and conn.faulted:
            conn.close()
            conn = None
        if not conn:
            conn = self.conn_pool[key] = self._new_connection(
                host, port, **kwargs)
        return conn


class ECDiskFileManager(DiskFileManager):
    pass


def install_kinetic_diskfile():
    kinetic_manager_map = {
        diskfile.REPL_POLICY: DiskFileManager,
        diskfile.EC_POLICY: ECDiskFileManager,
    }
    for policy_type, manager_class in kinetic_manager_map.items():
        diskfile.DiskFileRouter.policy_type_to_manager_cls.pop(
            policy_type, None)
        diskfile.DiskFileRouter.register(
            policy_type)(manager_class)


class ObjectController(server.ObjectController):

    def setup(self, conf):
        super(ObjectController, self).setup(conf)
        kinetic_logger = logging.getLogger('kinetic')
        for handler in self.logger.logger.handlers:
            kinetic_logger.addHandler(handler)


def app_factory(global_conf, **local_conf):
    install_kinetic_diskfile()
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
