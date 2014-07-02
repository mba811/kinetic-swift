import errno
import logging
import os
import sys
import shutil
import socket
import subprocess
import tempfile
import time
import unittest

import kinetic

from kinetic_swift.client import KineticSwiftClient

logging.basicConfig(level=logging.DEBUG)
kinetic_logger = logging.getLogger('kinetic')
kinetic_logger.addHandler(logging.NullHandler())
kinetic_logger.propagate = False


class FakeLogger(object):
    pass

for name in ('log', 'debug', 'info', 'warning', 'error', 'critical'):
    setattr(FakeLogger, name, getattr(logging, name))


JAR_PATH = os.environ['KINETIC_JAR']


def start_simulators(data_dir, *ports):
    sim_map = {}
    with open(os.devnull, 'w') as null:
        for port in ports:
            args = ['java', '-jar', JAR_PATH, str(port),
                    os.path.join(data_dir, str(port)), str(port + 443)]
            sim_map[port] = subprocess.Popen(args, stdout=null, stderr=null)
    time.sleep(1)
    connected = []
    backoff = 0.1
    timeout = time.time() + 3
    while len(connected) < len(sim_map) and time.time() < timeout:
        for port in sim_map:
            if port in connected:
                continue
            sock = socket.socket()
            try:
                sock.connect(('localhost', port))
            except socket.error:
                time.sleep(backoff)
                backoff *= 2
            else:
                connected.append(port)
                sock.close()
    if len(connected) < len(sim_map):
        teardown_simulators(sim_map)
        raise Exception('only able to connect to %r out of %r' % (connected,
                                                                  sim_map))
    return sim_map

def teardown_simulators(sim_map):
    for proc in sim_map.values():
        try:
            proc.terminate()
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise
            continue
        proc.wait()


class KineticSwiftTestCase(unittest.TestCase):

    PORTS = (9123,)

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.ports = self.PORTS
        self._sim_map = {}
        try:
            self._sim_map = start_simulators(self.test_dir, *self.ports)
            self.client_map = {}
            for port in self.ports:
                self.client_map[port] = KineticSwiftClient('localhost', port)
        except Exception:
            e, v, t = sys.exc_info()
            self.tearDown()
            raise e, v, t

    def tearDown(self):
        teardown_simulators(self._sim_map)
        shutil.rmtree(self.test_dir)

    def buildKey(self, key):
        return 'test/kinetic_swift/%s/%s' % (self.id(), key)


