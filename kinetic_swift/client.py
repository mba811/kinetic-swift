from collections import deque
import errno
from eventlet import Timeout, spawn_n, event

from kinetic.asyncclient import AsyncClient
import datetime


class Response(object):

    def __init__(self, client):
        self.resp = event.Event()
        self._hasError = False
        self.client = client

    def setResponse(self, v):
        self.resp.send(v)

    def setError(self, e):
        self._hasError = True
        self.resp.send(e)

    def ready(self):
        return self.resp.ready()

    def wait(self):
        try:
            with Timeout(self.client.response_timeout):
                try:
                    resp = self.resp.wait()
                    if self._hasError:
                        raise resp
                    else:
                        return resp
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

    def __init__(self, logger, host, port, **kwargs):
        self.host = self.hostname = host
        self.port = port
        self.response_timeout = kwargs.pop('response_timeout', 30)
        self.logger = logger
        self.conn = AsyncClient(host, port, **kwargs)
        self.conn.connect()

    def log_info(self, message):
        self.logger.info('%s kinetic %s (%s): %s' % (datetime.datetime.now(),
                                                     self.conn.hostname,
                                                     self.conn.connection_id,
                                                     message))

    def close(self):
        if not self.conn:
            return
        self.logger.warning('Foricing shutdown of connection to %s:%s' % (
            self.hostname, self.port))
        real_sock = None
        green_sock = getattr(self.conn, '_socket', None)
        if hasattr(green_sock, 'fd'):
            real_sock = getattr(green_sock.fd, '_sock', None)
        if self.conn and not self.conn.closing:
            self.conn.close()
        if real_sock:
            real_sock.close()
        self.logger.info('Connection to %s:%s is closed' % (
            self.hostname, self.port))
        self.conn = None

    @property
    def isConnected(self):
        return self.conn and self.conn.isConnected

    @property
    def faulted(self):
        if not self.conn:
            return True
        return self.conn.faulted

    def getPrevious(self, *args, **kwargs):
        # self.log_info('getPrevious')
        promise = Response(self)
        self.conn.getPreviousAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def put(self, key, data, *args, **kwargs):
        # self.log_info('put')
        promise = Response(self)
        self.conn.putAsync(promise.setResponse, promise.setError, key, data,
                           *args, **kwargs)
        return promise

    def getKeyRange(self, *args, **kwargs):
        # self.log_info('getKeyRange')
        promise = Response(self)
        self.conn.getKeyRangeAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def delete(self, key, *args, **kwargs):
        # self.log_info('delete')
        promise = Response(self)
        self.conn.deleteAsync(promise.setResponse, promise.setError, key,
                              *args, **kwargs)
        return promise

    def get(self, key, *args, **kwargs):
        # self.log_info('get')
        promise = Response(self)
        self.conn.getAsync(promise.setResponse, promise.setError, key,
                           *args, **kwargs)
        return promise

    def raise_err(self, *args, **kwargs):
        raise Exception(
            'error handling request to Drive %s:%s : %r %r' % (
                self.host, self.port, args, kwargs))

    def rename(self, key, new_key):
        promise = Response(self)

        def delete_key(*args, **kwargs):
            self.conn.deleteAsync(promise.setResponse, promise.setError, key,
                                  *args, **kwargs)

        def write_entry(entry):
            if not entry:
                delete_key()
            else:
                self.conn.putAsync(delete_key, self.raise_err,
                                   new_key, entry.value)

        self.conn.getAsync(write_entry, self.raise_err, key)
        return promise

    def copy_keys(self, target, keys, depth=16):
        # self.log_info('copy_keys')
        host, port = target.split(':')
        target = self.__class__(self.logger, host, int(port))

        def write_entry(entry):
            target.put(entry.key, entry.value, force=True)

        for key in keys:
            self.conn.getAsync(write_entry, self.raise_err, key)
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
