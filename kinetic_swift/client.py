from kinetic.greenclient import GreenClient
from kinetic import operations

class KineticSwiftClient(GreenClient):

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
