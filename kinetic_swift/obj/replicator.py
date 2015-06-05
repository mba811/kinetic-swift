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
from swift.common.storage_policy import (
    POLICIES, EC_POLICY, get_policy_string, split_policy_string)

from kinetic_swift.client import KineticSwiftClient
from kinetic_swift.obj.server import object_key, install_kinetic_diskfile


def split_key(key):
    parts = key.split('.')
    _base, policy = split_policy_string(parts[0])
    hashpath = parts[1]
    timestamp = '.'.join(parts[2:4])
    nonce = parts[-1]
    return {
        'policy': policy,
        'hashpath': hashpath,
        'nonce': nonce,
        'timestamp': timestamp,
    }


class KineticReplicator(ObjectReplicator):

    def __init__(self, conf):
        install_kinetic_diskfile()
        super(KineticReplicator, self).__init__(conf)
        self.replication_mode = conf.get('kinetic_replication_mode', 'push')
        self.connect_timeout = int(conf.get('connect_timeout', 3))
        self.response_timeout = int(conf.get('response_timeout', 30))
        # device => [last_used, conn]
        self._conn_pool = {}
        self.max_connections = 10

    def iter_all_objects(self, conn, policy):
        prefix = get_policy_string('objects', policy)
        key_range = [prefix + term for term in ('.', '/')]
        keys = conn.getKeyRange(*key_range).wait()
        while keys:
            for key in keys:
                # TODO: clean up hashdir and old tombstones
                yield key
            # see if there's any more values
            key_range[0] = key
            keys = conn.getKeyRange(*key_range,
                                    startKeyInclusive=False).wait()

    def find_target_devices(self, key, policy):
        key_info = split_key(key)
        # ring magic, find all of the nodes for the partion of the given hash
        raw_digest = key_info['hashpath'].decode('hex')
        part = struct.unpack_from('>I', raw_digest)[0] >> \
            policy.object_ring._part_shift
        devices = policy.object_ring.get_part_nodes(part)
        return [d['device'] for d in devices]

    def iter_object_keys(self, conn, key):
        yield key
        key_info = split_key(key)
        chunk_key = 'chunks.%(hashpath)s.%(nonce)s' % key_info
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
        key_info = split_key(key)
        key = object_key(key_info['policy'], key_info['hashpath'])

        conn = self.get_conn(target)
        entry = conn.getPrevious(key).wait()
        if not entry:
            return False
        target_key_info = split_key(entry.key)
        if target_key_info['hashpath'] != key_info['hashpath']:
            return False
        if target_key_info['timestamp'] < key_info['timestamp']:
            return False
        return True

    def get_conn(self, device):
        now = time.time()
        try:
            pool_entry = self._conn_pool[device]
        except KeyError:
            self.logger.debug('Creating new connection to %r', device)
            self._close_old_connections()
            conn = self._get_conn(device)
        else:
            conn = pool_entry[1]
        self._conn_pool[device] = (now, conn)
        return conn

    def _close_old_connections(self):
        oldest_keys = sorted(self._conn_pool, key=lambda k:
                             self._conn_pool[k][0])
        while len(self._conn_pool) > self.max_connections:
            device = oldest_keys.pop(0)
            pool_entry = self._conn_pool.pop(device)
            last_used, conn = pool_entry
            self.logger.debug('Closing old connection to %r (%s)', device,
                              time.time() - last_used)
            conn.close()

    def _get_conn(self, device):
        host, port = device.split(':')
        conn = KineticSwiftClient(
            self.logger, host, int(port),
            connect_timeout=self.connect_timeout,
            response_timeout=self.response_timeout,
        )
        return conn

    def reconstruct_fa(self, conn, key, target):
        # get object info from conn
        # find target in ring
        # internal client GET
        # direct client PUT
        return

    def replicate_object(self, conn, key, targets, delete=False):
        keys = None
        success = 0
        for target in targets:
            try:
                if self.is_object_on_target(target, key):
                    success += 1
                    continue
                # TODO: if EC & not delete:  reconstruct_fa
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

    def replicate_device(self, device, conn, policy):
        self.logger.info('begining replication pass for %r', device)
        for key in self.iter_all_objects(conn, policy):
            # might be a good place to collect jobs and group by
            # partition and/or target
            targets = list(self.find_target_devices(key, policy))
            try:
                targets.remove(device)
            except ValueError:
                # device is not a target
                delete = True
            else:
                # object is supposed to be here
                delete = False
            self.replicate_object(conn, key, targets, delete=delete)

    def _replicate(self, *devices, **kwargs):
        policy = kwargs.get('policy', POLICIES.legacy)
        for device in devices:
            try:
                # might be a good place to go multiprocess
                try:
                    conn = self.get_conn(device)
                except:
                    self.logger.exception(
                        'Unable to connect to device: %r', device)
                    continue
                try:
                    self.replicate_device(device, conn, policy)
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
            # TODO: let EC go through
            if policy.policy_type == EC_POLICY:
                continue
            obj_ring = self.load_object_ring(policy)
            devices = override_devices or [d['device'] for d in
                                           obj_ring.devs if d]
            self.logger.debug(_("Begin replication for %r"), policy)
            try:
                self._replicate(*devices, policy=policy)
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
