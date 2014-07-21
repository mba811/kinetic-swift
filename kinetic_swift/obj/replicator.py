import errno
from optparse import OptionParser
import os
import socket
import sys
import time
import struct

from swift.common.utils import parse_options
from swift.common.daemon import run_daemon
from swift.obj.replicator import ObjectReplicator
from swift import gettext_ as _
from swift.common.storage_policy import POLICIES

from kinetic_swift.client import KineticSwiftClient
from kinetic_swift.obj.server import object_key


def split_key(key):
    parts = key.split('.')
    storage_policy = parts[0]
    if '-' in storage_policy:
        base, policy_index = storage_policy.split('-', 1)
    else:
        policy_index = 0
    hashpath = parts[1]
    nounce = parts[-1]
    return policy_index, hashpath, nounce


class KineticReplicator(ObjectReplicator):

    def __init__(self, conf):
        super(KineticReplicator, self).__init__(conf)
        self.replication_mode = conf.get('kinetic_replication_mode', 'push')

    def iter_all_objects(self, conn):
        keys = conn.getKeyRange('objects,', 'objects/')
        for key in keys.wait():
            # FIXME: clean up old tombstones
            yield key

    def find_target_devices(self, key):
        policy_index, hashpath, _junk = split_key(key)
        object_ring = self.get_object_ring(policy_index)
        # ring magic, find all of the nodes for the partion of the given hash
        raw_digest = hashpath.decode('hex')
        part = struct.unpack_from('>I', raw_digest)[0] >> \
            object_ring._part_shift
        devices = object_ring.get_part_nodes(part)
        return [d['device'] for d in devices]

    def iter_object_keys(self, conn, key):
        yield key
        policy_index, hashpath, nounce = split_key(key)
        chunk_key = 'chunks.%s.%s' % (hashpath, nounce)
        resp = conn.getKeyRange(chunk_key + '.', chunk_key + '/')
        for key in resp.wait():
            yield key

    def replicate_object_to_target(self, conn, keys, target):
        if self.replication_mode == 'push':
            conn.push_keys(target, keys)
        else:
            conn.copy_keys(target, keys)

    def is_object_on_target(self, target, key):
        # get key ready for getPrevious on target
        policy_index, hashpath, _nounce = split_key(key)
        key = object_key(policy_index, hashpath)

        conn = self.get_conn(target)
        with conn:
            entry = conn.getPrevious(key).wait()
        return entry and entry.key.startswith(key[:-1])

    def get_conn(self, device):
        host, port = device.split(':')
        conn = KineticSwiftClient(host, int(port))
        return conn

    def replicate_object(self, conn, key, targets, delete=False):
        keys = None
        success = 0
        for target in targets:
            try:
                if self.is_object_on_target(target, key):
                    success += 1
                    continue
                keys = keys or list(self.iter_object_keys(conn, key))
                self.replicate_object_to_target(conn, keys, target)
            except Exception:
                self.logger.exception('Unable to replicate %r to %r',
                                      key, target)
            else:
                self.logger.info('successfully replicated %r to %r',
                                 key, target)
                success += 1
        if delete and success >= len(targets):
            # might be nice to drop the whole partition at once
            keys = keys or list(self.iter_object_keys(conn, key))
            conn.delete_keys(keys)
            self.logger.info('successfully removed handoff %r to %r',
                             key, targets)

    def replicate_device(self, device, conn):
        self.logger.info('begining replication pass for %r', device)
        for key in self.iter_all_objects(conn):
            # might be a good place to collect jobs and group by
            # partition and/or target
            targets = list(self.find_target_devices(key))
            try:
                targets.remove(device)
            except ValueError:
                # device is not a target
                delete = True
            else:
                # object is supposed to be here
                delete = False
            self.replicate_object(conn, key, targets, delete=delete)

    def _replicate(self, *devices):
        for device in devices:
            try:
                # might be a good place to go multiprocess
                conn = self.get_conn(device)
                try:
                    with conn as conn:
                        self.replicate_device(device, conn)
                except socket.error as e:
                    if e.errno != errno.ECONNREFUSED:
                        raise
                    self.logger.error('Connection refused for %r', device)
            except Exception:
                self.logger.exception('Unhandled exception with '
                                      'replication for device %r', device)

    def replicate(self, override_devices=None, **kwargs):
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.replication_count = 0
        self.last_replication_count = -1
        self.partition_times = []
        for policy in POLICIES:
            obj_ring = self.get_object_ring(policy.idx)
            devices = override_devices or [d['device'] for d in
                                           obj_ring.devs]
            self.logger.debug(_("Begin replication for %r"), policy)
            try:
                self._replicate(*devices)
            except Exception:
                self.logger.exception(
                    _("Exception in top-level replication loop"))
        self.logger.info('replication cycle for %r complete', devices)


def main():
    try:
        if not os.path.exists(sys.argv[1]):
            sys.argv.insert(1, '/etc/swift/kinetic.conf')
    except IndexError:
        pass
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help='Replicate only given devices. '
                           'Comma-separated list')
    conf_file, options = parse_options(parser, once=True)
    run_daemon(KineticReplicator, conf_file,
               section_name='object-replicator', **options)


if __name__ == "__main__":
    sys.exit(main())
