from kinetic.asyncclient import AsyncClient
from kinetic.greenclient import Response
from kinetic import operations


class KineticSwiftClient(object):

    def __init__(self, host, port, **kwargs):
        self.conn = AsyncClient(host, port, **kwargs)
        self.conn.connect()

    @property
    def hostname(self):
        return self.conn.hostname

    @property
    def port(self):
        return self.conn.port

    def close(self):
        real_sock = None
        green_sock = self.conn._socket
        if hasattr(green_sock, 'fd'):
            real_sock = getattr(green_sock.fd, '_sock', None)
        self.conn.close()
        if real_sock:
            real_sock.close()
        self.conn = None

    @property
    def isConnected(self):
        return self.conn and self.conn.isConnected

    def getPrevious(self, *args, **kwargs):
        promise = Response()
        self.conn.getPreviousAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def put(self, key, data, *args, **kwargs):
        promise = Response()
        self.conn.putAsync(promise.setResponse, promise.setError, key, data,
                           *args, **kwargs)
        return promise

    def getKeyRange(self, *args, **kwargs):
        promise = Response()
        self.conn.getKeyRangeAsync(promise.setResponse, promise.setError,
                                   *args, **kwargs)
        return promise

    def delete(self, key, *args, **kwargs):
        promise = Response()
        self.conn.deleteAsync(promise.setResponse, promise.setError, key,
                              *args, **kwargs)
        return promise

    def get(self, key, *args, **kwargs):
        promise = Response()
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
