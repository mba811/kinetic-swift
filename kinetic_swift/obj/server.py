import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'cpp'
from contextlib import contextmanager
from collections import deque
from uuid import uuid4
import socket

import msgpack
from swift.obj import diskfile, server

from kinetic_swift.client import KineticSwiftClient

DEFAULT_DEPTH = 16


def chunk_key(hashpath, nounce, index):
    return 'chunks.%s.%s.%0.32d' % (hashpath, nounce, index)


def object_key(hashpath, timestamp=None, extension='.data', nounce=''):
    if timestamp:
        return 'objects.%s.%s%s.%s' % (hashpath, timestamp, extension, nounce)
    else:
        # for use with getPrevious
        return 'objects.%s/' % (hashpath)

def get_nounce(key):
    return key.rsplit('.', 1)[-1]


class DiskFile(diskfile.DiskFile):

    def __init__(self, root, device, *args, **kwargs):
        super(DiskFile, self).__init__(root, device, *args, **kwargs)
        host, port = device.split(':')
        self.conn = KineticSwiftClient(host, int(port))
        self.hashpath = os.path.basename(self.datadir.rstrip(os.path.sep))
        self._buffer = ''
        # this is the first "disk_chunk_size" + metadata
        self._headbuffer = None
        self._nounce = None
        self.upload_size = 0
        self.last_sync = 0
        # configurables
        self.write_depth = DEFAULT_DEPTH
        try:
            self.conn.connect()
        except socket.error:
            raise diskfile.DiskFileDeviceUnavailable(
                'unable to connect to %s:%s' % (
                    self.conn.hostname, self.conn.port))

    def open(self, **kwargs):
        self._read()
        return self

    def close(self, **kwargs):
        self.conn.close()
        self._metadata = None

    def _read(self):
        key = object_key(self.hashpath)
        entry = self.conn.getPrevious(key).wait()
        if not entry or not entry.key.startswith(key[:-1]):
            self._metadata = {}  # mark object as "open"
            return
        self.data_file = '.ts.' not in entry.key
        blob = entry.value
        self._nounce = get_nounce(entry.key)
        payload = msgpack.unpackb(blob)
        self._metadata = payload['metadata']
        self._headbuffer = payload['buffer']

    def __iter__(self):
        if not self._metadata:
            return
        yield self._headbuffer
        keys = [chunk_key(self.hashpath, self._nounce, i + 1) for i in
                range(int(self._metadata['X-Kinetic-Chunk-Count']))]
        for entry in self.conn.get_keys(keys):
            yield entry.value

    @contextmanager
    def create(self, size=None):
        self._headbuffer = None
        self._nounce = str(uuid4())
        try:
            self.conn.connect()
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

    def _submit_write(self, key, blob):
        if len(self._pending_write) >= self.write_depth:
            self._pending_write.popleft().wait()
        pending_resp = self.conn.put(key, blob, force=True)
        self._pending_write.append(pending_resp)

    def _sync_buffer(self):
        if not self._headbuffer:
            # save the headbuffer
            self._headbuffer = self._buffer
            self._chunk_id = 0
        elif self._buffer:
            # write out the chunk buffer!
            self._chunk_id += 1
            key = chunk_key(self.hashpath, self._nounce, self._chunk_id)
            self._submit_write(key, self._buffer)
        self._buffer = ''

    def _wait_write(self):
        for resp in self._pending_write:
            resp.wait()

    def put(self, metadata, extension='.data'):
        if extension == '.ts':
            metadata['deleted'] = True
        self._sync_buffer()
        # zero index, chunk-count is len
        metadata['X-Kinetic-Chunk-Count'] = self._chunk_id
        metadata['X-Kinetic-Chunk-Nounce'] = self._nounce
        metadata['name'] = self.name
        self._metadata = metadata
        payload = {'metadata': metadata, 'buffer': self._headbuffer}
        blob = msgpack.packb(payload)
        timestamp = diskfile.normalize_timestamp(metadata['X-Timestamp'])
        key = object_key(self.hashpath, timestamp, extension, self._nounce)
        self._submit_write(key, blob)
        self._wait_write()
        self._unlink_old(timestamp)

    def _unlink_old(self, req_timestamp):
        start_key = object_key(self.hashpath)[:-1]
        end_key = object_key(self.hashpath, timestamp=req_timestamp)
        resp = self.conn.getKeyRange(start_key, end_key, endKeyInclusive=False)
        head_keys = resp.wait()
        for key in head_keys:
            nounce = get_nounce(key)
            def key_gen():
                yield key
                i = 1
                while True:
                    missing = yield chunk_key(self.hashpath, nounce, i)
                    i += 1
                    if missing:
                        break
            self.conn.delete_keys(key_gen(), depth=4)

    def quarantine(self):
        pass

    def get_data_file_size(self):
        return self._metadata['Content-Length']


class ObjectController(server.ObjectController):

    def _diskfile(self, device, partition, account, container, obj, **kwargs):
        """Utility method for instantiating a DiskFile."""
        kwargs.setdefault('mount_check', self.mount_check)
        kwargs.setdefault('bytes_per_sync', self.bytes_per_sync)
        kwargs.setdefault('disk_chunk_size', self.disk_chunk_size)
        kwargs.setdefault('threadpool', self.threadpools[device])
        kwargs.setdefault('obj_dir', server.DATADIR)
        return DiskFile(self.devices, device, partition, account,
                        container, obj, self.logger, **kwargs)


def app_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
