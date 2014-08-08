import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'cpp'
import logging
from contextlib import contextmanager
from collections import deque
from uuid import uuid4
from eventlet import sleep, Timeout, spawn_n

import msgpack
from swift.obj import diskfile, server

from kinetic_swift.client import KineticSwiftClient


DEFAULT_DEPTH = 2

SYNC_INVALID = -1
SYNC_WRITETHROUGH = 1
SYNC_WRITEBACK = 2
SYNC_FLUSH = 3

SYNC_OPTION_MAP = {
    'default': SYNC_INVALID,
    'writethrough': SYNC_WRITETHROUGH,
    'writeback': SYNC_WRITEBACK,
    'flush': SYNC_FLUSH,
}


def chunk_key(hashpath, nounce, index=None):
    if index is None:
        # for use with getKeyRange
        key = 'chunks.%s.%s/' % (hashpath, nounce)
    else:
        key = 'chunks.%s.%s.%0.32d' % (hashpath, nounce, index)
    return key


def object_key(policy_index, hashpath, timestamp='',
               extension='.data', nounce=''):
    storage_policy = diskfile.get_data_dir(policy_index)
    if timestamp:
        return '%s.%s.%s%s.%s' % (storage_policy, hashpath, timestamp,
                                  extension, nounce)
    else:
        # for use with getPrevious
        return '%s.%s/' % (storage_policy, hashpath)


def get_nounce(key):
    return key.rsplit('.', 1)[-1]


class DiskFileManager(diskfile.DiskFileManager):

    def __init__(self, conf, logger):
        super(DiskFileManager, self).__init__(conf, logger)
        self.connect_timeout = int(conf.get('connect_timeout', 3))
        self.response_timeout = int(conf.get('response_timeout', 30))
        self.connect_retry = int(conf.get('connect_retry', 3))
        self.write_depth = int(conf.get('write_depth', DEFAULT_DEPTH))
        self.read_depth = int(conf.get('read_depth', DEFAULT_DEPTH))
        self.delete_depth = int(conf.get('delete_depth', DEFAULT_DEPTH))
        raw_sync_option = conf.get('synchronization', 'default').lower()
        try:
            self.synchronization = SYNC_OPTION_MAP[raw_sync_option]
        except KeyError:
            raise ValueError('Invalid synchronization option, choices are %r' %
                             SYNC_OPTION_MAP.keys())
        self.conn_pool = {}

    def get_diskfile(self, device, *args, **kwargs):
        host, port = device.split(':')
        return DiskFile(self, host, port, self.threadpools[device], *args,
                        **kwargs)

    def pickle_async_update(self, device, account, container, obj, data,
                            timestamp, policy_idx):
        pass

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


class DiskFileReader(diskfile.DiskFileReader):

    def __init__(self, diskfile):
        self.diskfile = diskfile

    def __iter__(self):
        return iter(self.diskfile)

    def close(self):
        return self.diskfile.close()


class DiskFile(diskfile.DiskFile):

    def __init__(self, mgr, host, port, *args, **kwargs):
        device_path = ''
        self.disk_chunk_size = kwargs.pop('disk_chunk_size',
                                          mgr.disk_chunk_size)
        self.policy_index = kwargs.get('policy_idx', 0)
        # this is normally setup in DiskFileWriter, but we do it here
        self._extension = '.data'
        # this is to neuter the context manager close in GET
        self._took_reader = False
        super(DiskFile, self).__init__(mgr, device_path, *args, **kwargs)
        self.hashpath = os.path.basename(self._datadir.rstrip('/'))
        self._buffer = ''
        # this is the first "disk_chunk_size" + metadata
        self._nounce = None
        self.upload_size = 0
        self.last_sync = 0
        # configurables
        self.write_depth = self._mgr.write_depth
        self.read_depth = self._mgr.read_depth
        self.delete_depth = self._mgr.delete_depth
        self.synchronization = self._mgr.synchronization
        self.conn = None
        self.conn = mgr.get_connection(host, port)

    def object_key(self, *args, **kwargs):
        return object_key(self.policy_index, self.hashpath, *args, **kwargs)

    def _read(self):
        key = self.object_key()
        entry = self.conn.getPrevious(key).wait()
        if not entry or not entry.key.startswith(key[:-1]):
            self._metadata = {}  # mark object as "open"
            return
        self.data_file = '.ts.' not in entry.key
        blob = entry.value
        self._nounce = get_nounce(entry.key)
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
        return self

    def close(self, **kwargs):
        self._metadata = None

    def __exit__(self, t, v, tb):
        if not self._took_reader:
            self.close()

    def __iter__(self):
        if not self._metadata:
            return
        keys = [chunk_key(self.hashpath, self._nounce, i + 1) for i in
                range(int(self._metadata['X-Kinetic-Chunk-Count']))]

        pending = deque()
        for key in keys:
            while len(pending) >= self.read_depth:
                entry = pending.popleft().wait()
                yield str(entry.value)
            pending.append(self.conn.get(key))
        for resp in pending:
            entry = resp.wait()
            yield str(entry.value)

    @contextmanager
    def create(self, size=None):
        self._nounce = str(uuid4())
        self._chunk_id = 0
        try:
            self._pending_write = deque()
            yield self
        finally:
            self.close()

    def write(self, chunk):
        self._buffer += chunk
        self.upload_size += len(chunk)

        diff = self.upload_size - self.last_sync
        if diff >= self.disk_chunk_size:
            self._sync_buffer()
            self.last_sync = self.upload_size
        return self.upload_size

    def _submit_write(self, key, blob, final=True):
        if len(self._pending_write) >= self.write_depth:
            self._pending_write.popleft().wait()
        if self.synchronization == SYNC_FLUSH and not final:
            synchronization = SYNC_WRITEBACK
        else:
            synchronization = self.synchronization
        pending_resp = self.conn.put(key, blob, force=True,
                                     synchronization=synchronization)
        self._pending_write.append(pending_resp)

    def _sync_buffer(self):
        if self._buffer:
            # write out the chunk buffer!
            self._chunk_id += 1
            key = chunk_key(self.hashpath, self._nounce, self._chunk_id)
            self._submit_write(key, self._buffer[:self.disk_chunk_size],
                               final=False)
        self._buffer = self._buffer[self.disk_chunk_size:]

    def _wait_write(self):
        for resp in self._pending_write:
            resp.wait()

    def put(self, metadata):
        if self._extension == '.ts':
            metadata['deleted'] = True
        self._sync_buffer()
        while self._buffer:
            self._sync_buffer()
        # zero index, chunk-count is len
        metadata['X-Kinetic-Chunk-Count'] = self._chunk_id
        metadata['X-Kinetic-Chunk-Nounce'] = self._nounce
        metadata['name'] = self._name
        self._metadata = metadata
        blob = msgpack.packb(self._metadata)
        timestamp = diskfile.Timestamp(metadata['X-Timestamp'])
        key = self.object_key(timestamp.internal, self._extension,
                              self._nounce)
        self._submit_write(key, blob, final=True)
        self._wait_write()
        spawn_n(self._unlink_old, timestamp)

    def _unlink_old(self, req_timestamp):
        start_key = self.object_key()[:-1]
        end_key = self.object_key(timestamp=req_timestamp.internal)
        resp = self.conn.getKeyRange(start_key, end_key, endKeyInclusive=False)
        head_keys = resp.wait()
        pending = deque()
        for head_key in head_keys:
            nounce = get_nounce(head_key)

            def key_gen():
                start_key = chunk_key(self.hashpath, nounce, 0)
                end_key = chunk_key(self.hashpath, nounce)
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
        pass

    def get_data_file_size(self):
        return self._metadata['Content-Length']


class ObjectController(server.ObjectController):

    def setup(self, conf):
        super(ObjectController, self).setup(conf)
        self._diskfile_mgr = DiskFileManager(conf, self.logger)
        kinetic_logger = logging.getLogger('kinetic')
        for handler in self.logger.logger.handlers:
            kinetic_logger.addHandler(handler)


def app_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
