#!/usr/bin/env python

from collections import defaultdict
from optparse import OptionParser
import os
import random
import sys
import time

import msgpack

from swift.common.daemon import run_daemon
from swift.common.storage_policy import POLICIES
from swift.common.swob import HeaderKeyDict
from swift.common.utils import parse_options, list_from_csv
from swift.obj.updater import ObjectUpdater, dump_recon_cache
from swift import gettext_ as _

from kinetic_swift.obj.server import DiskFileManager


class KineticUpdater(ObjectUpdater):

    def __init__(self, *args, **kwargs):
        super(KineticUpdater, self).__init__(*args, **kwargs)
        self.stats = defaultdict(int)
        self.mgr = DiskFileManager(self.conf, self.logger)

    def run_forever(self, *args, **kwargs):
        """Run the updater continuously."""
        time.sleep(random.random() * self.interval)
        while True:
            begin = time.time()
            self.logger.info(_('Begin object update sweep'))
            self.run_once(*args, **kwargs)
            elapsed = time.time() - begin
            self.logger.info(_('Object update sweep completed: %.02fs'),
                             elapsed)
            dump_recon_cache({'object_updater_sweep': elapsed},
                             self.rcache, self.logger)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.stats = defaultdict(int)

    def _get_devices(self):
        return set([
            d['device'] for policy in POLICIES for d in
            POLICIES.get_object_ring(int(policy), self.swift_dir).devs
            if d
        ])

    def run_once(self, *args, **kwargs):
        override_devices = list_from_csv(kwargs.get('devices'))
        devices = override_devices or self._get_devices()
        for device in devices:
            self.object_sweep(device)

    def _find_updates_entries(self, device):
        conn = self.mgr.get_connection(*device.split(':'))
        start_key = 'async_pending'
        end_key = 'async_pending/'
        for async_key in conn.getKeyRange(start_key, end_key).wait():
            yield async_key

    def object_sweep(self, device):
        self.logger.debug('Search async_pending on %r', device)
        for update_entry in self._find_updates_entries(device):
            self.stats['found_updates'] += 1
            success = self.process_object_update(device, update_entry)
            if success:
                self.stats['success'] += 1
            else:
                self.stats['failures'] += 1

    def _load_update(self, device, async_key):
        # load update
        conn = self.mgr.get_connection(*device.split(':'))
        resp = conn.get(async_key)
        entry = resp.wait()
        update = msgpack.unpackb(entry.value)
        return update

    def _unlink_update(self, device, async_key):
        conn = self.mgr.get_connection(*device.split(':'))
        conn.delete(async_key).wait()
        return True

    def _save_update(self, device, async_key, update):
        conn = self.mgr.get_connection(*device.split(':'))
        blob = msgpack.packb(update)
        conn.put(async_key, blob).wait()
        return True

    def process_object_update(self, device, update_entry):
        update = self._load_update(device, update_entry)

        # process update
        headers = HeaderKeyDict(update['headers'])
        del headers['user-agent']
        successes = update.get('successes', [])
        part, nodes = self.get_container_ring().get_nodes(
            update['account'], update['container'])
        obj = '/%s/%s/%s' % \
              (update['account'], update['container'], update['obj'])
        success = True
        new_successes = False
        for node in nodes:
            if node['id'] not in successes:
                new_success, node_id = self.object_update(
                    node, part, update['op'], obj, headers)
                if new_success:
                    successes.append(node['id'])
                    new_successes = True
                else:
                    success = False
        if success:
            self.successes += 1
            self.logger.increment('successes')
            self.logger.debug('Update sent for %(obj)s %(path)s',
                              {'obj': obj, 'path': update_entry})
            self.logger.increment("unlinks")
            return self._unlink_update(device, update_entry)
        else:
            self.failures += 1
            self.logger.increment('failures')
            self.logger.debug('Update failed for %(obj)s %(path)s',
                              {'obj': obj, 'path': update_entry})
            if new_successes:
                update['successes'] = successes
                self._save_update(device, update_entry, update)
        return success


def main():
    try:
        if not os.path.exists(sys.argv[1]):
            sys.argv.insert(1, '/etc/swift/kinetic.conf')
    except IndexError:
        pass
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help='Update only given devices. '
                           'Comma-separated list')
    conf_file, options = parse_options(parser, once=True)
    run_daemon(KineticUpdater, conf_file,
               section_name='object-updater', **options)


if __name__ == "__main__":
    sys.exit(main())
