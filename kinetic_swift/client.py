import errno
from eventlet import Timeout, spawn_n

from kinetic.asyncclient import AsyncClient
from kinetic.greenclient import Response as BaseResponse
from kinetic import operations


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

    def __init__(self, logger, host, port, **kwargs):
        self.host = self.hostname = host
        self.port = port
        self.response_timeout = kwargs.pop('response_timeout', 30)
        self.logger = logger
        self.conn = AsyncClient(host, port, **kwargs)
        self.conn.connect()

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
        promise = Response(self)
        self.conn.getPreviousAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def put(self, key, data, *args, **kwargs):
        promise = Response(self)
        self.conn.putAsync(promise.setResponse, promise.setError, key, data,
                           *args, **kwargs)
        return promise

    def getKeyRange(self, *args, **kwargs):
        promise = Response(self)
        self.conn.getKeyRangeAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def delete(self, key, *args, **kwargs):
        promise = Response(self)
        self.conn.deleteAsync(promise.setResponse, promise.setError, key,
                              *args, **kwargs)
        return promise

    def get(self, key, *args, **kwargs):
        promise = Response(self)
        self.conn.getAsync(promise.setResponse, promise.setError, key,
                           *args, **kwargs)
        return promise

    def copy_keys(self, target, keys, depth=16):
        host, port = target.split(':')
        target = self.__class__(host, int(port))

        def write_entry(entry):
            target.put(entry.key, entry.value, force=True)

        def blow_up(*args, **kwargs):
            raise Exception('do something %r %r' % (args, kwargs))

        with target:
            for key in keys:
                self._processAsync(operations.Get, write_entry, blow_up, key)
            self.wait()
