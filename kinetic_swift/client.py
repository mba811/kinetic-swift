from collections import deque
import errno
from eventlet import Timeout, spawn_n, sleep, semaphore

from swift.obj import diskfile

from kinetic.asyncclient import AsyncClient
from kinetic.greenclient import Response as BaseResponse
import datetime


class Response(BaseResponse):

    def __init__(self, client):
        self.client = client
        super(Response, self).__init__()

    def wait(self):
        try:
            with Timeout(self.client.response_timeout):
                try:
                    return super(Response, self).wait()
                except OSError as e:
                    if e.errno == errno.ECONNRESET:
                        self.client.logger.error('Drive reset connection')
                        self.client.close()
                    raise
        except Timeout:
            spawn_n(self.client.close)
            raise Exception('Timeout (%ss) getting response from Drive %s:%s' %
                            (self.client.response_timeout,
                             self.client.host, self.client.port))


class KineticSwiftClient(object):

    def __init__(self, logger, host, port,
                 connect_timeout=3, connect_retry=3, response_timeout=30):
        self.host = self.hostname = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.connect_retry = connect_retry
        self.response_timeout = response_timeout
        self.logger = logger
        self.conns = [None, None]
        self.sem = semaphore.Semaphore()
        self.connect()

    @property
    def conn(self):
        if self.faulted or not self.isConnected:
            self.connect()
        return self.conns[0]

    @property
    def metadata_conn(self):
        if self.faulted or not self.isConnected:
            self.connect()
        return self.conns[1]

    def _new_connection(self):
        for i in range(1, self.connect_retry + 1):
            try:
                conn = AsyncClient(self.host, self.port,
                                   connect_timeout=self.connect_timeout)
                conn.connect()
                return conn
            except Timeout:
                self.logger.warning('Drive %s:%s connect timeout #%d (%ds)' % (
                    self.host, self.port, i, self.connect_timeout))
            except Exception:
                self.logger.exception('Drive %s:%s connection error #%d' % (
                    self.host, self.port, i))
            if i < self.connect_retry:
                sleep(1)
        msg = 'Unable to connect to drive %s:%s after %s attempts' % (
            self.host, self.port, i)
        self.logger.error(msg)
        raise diskfile.DiskFileDeviceUnavailable()

    def connect(self):
        with self.sem:
            for i, conn in enumerate(self.conns):
                if conn and conn.faulted:
                    conn.close()
                    conn = None
                if conn:
                    continue
                conn = self._new_connection()
                self.conns[i] = conn

    def log_info(self, message):
        self.logger.info('%s kinetic %s (%s): %s' % (datetime.datetime.now(),
                                                     self.conn.hostname,
                                                     self.conn.connection_id,
                                                     message))

    def _close(self):
        conns, self.conns = self.conns, [None, None]
        if not any(conns):
            return
        self.logger.warning('Foricing shutdown of connection to %s:%s' % (
            self.hostname, self.port))
        for conn in conns:
            if not conn:
                continue
            real_sock = None
            green_sock = getattr(conn, '_socket', None)
            if hasattr(green_sock, 'fd'):
                real_sock = getattr(green_sock.fd, '_sock', None)
            if conn and not conn.closing:
                conn.close()
            if real_sock:
                real_sock.close()
            self.logger.info('Connection to %s:%s is closed' % (
                self.hostname, self.port))

    def close(self):
        with self.sem:
            self._close()

    @property
    def isConnected(self):
        return any(self.conns) and all(conn.isConnected for conn in self.conns)

    @property
    def faulted(self):
        if not all(self.conns):
            return True
        return any(conn.faulted for conn in self.conns)

    def getPrevious(self, *args, **kwargs):
        # self.log_info('getPrevious')
        promise = Response(self)
        self.metadata_conn.getPreviousAsync(promise.setResponse,
                                            promise.setError,
                                            *args, **kwargs)
        return promise

    def put(self, key, data, *args, **kwargs):
        # self.log_info('put')
        promise = Response(self)
        self.conn.putAsync(promise.setResponse, promise.setError, key, data,
                           *args, **kwargs)
        return promise

    def put_metadata(self, key, data, *args, **kwargs):
        # self.log_info('put')
        promise = Response(self)
        self.metadata_conn.putAsync(promise.setResponse, promise.setError,
                                    key, data, *args, **kwargs)
        return promise

    def getKeyRange(self, *args, **kwargs):
        # self.log_info('getKeyRange')
        promise = Response(self)
        self.metadata_conn.getKeyRangeAsync(promise.setResponse,
                                            promise.setError,
                                            *args, **kwargs)
        return promise

    def delete(self, key, *args, **kwargs):
        # self.log_info('delete')
        promise = Response(self)
        self.metadata_conn.deleteAsync(promise.setResponse,
                                       promise.setError,
                                       key, *args, **kwargs)
        return promise

    def get(self, key, *args, **kwargs):
        # self.log_info('get')
        promise = Response(self)
        self.conn.getAsync(promise.setResponse, promise.setError, key,
                           *args, **kwargs)
        return promise

    def copy_keys(self, target, keys, depth=16):
        # self.log_info('copy_keys')
        host, port = target.split(':')
        target = self.__class__(self.logger, host, int(port))

        def write_entry(entry):
            target.put(entry.key, entry.value, force=True)

        def blow_up(*args, **kwargs):
            raise Exception('do something %r %r' % (args, kwargs))

        for key in keys:
            self.conn.getAsync(write_entry, blow_up, key)
        self.conn.wait()
        target.conn.wait()

    def delete_keys(self, keys, depth=16):
        # self.log_info('delete_keys')
        pending = deque()
        for key in keys:
            while len(pending) >= depth:
                found = pending.popleft().wait()
                if not found:
                    break
            pending.append(self.delete(key, force=True))

        for resp in pending:
            resp.wait()

    def push_keys(self, target, keys, batch=16):
        # self.log_info('push_keys')
        host, port = target.split(':')
        port = int(port)
        key_batch = []
        results = []
        for key in keys:
            key_batch.append(key)
            if len(key_batch) < batch:
                continue
            # send a batch
            results.extend(self.conn.push(key_batch, host, port))

            key_batch = []
        if key_batch:
            results.extend(self.conn.push(key_batch, host, port))
        return results
