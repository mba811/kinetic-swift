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

from device import MemcachedDeviceMap
from client import KineticSwiftClient
from swift.common.exceptions import DiskFileDeviceUnavailable

class ConnectionManager(object):
	
	def __init__(self, conf, logger):
        self.logger = logger
        self.persist_connection = bool(conf.get('persist_connection', False))
        self.connect_timeout = int(conf.get('connect_timeout', 3))
        self.response_timeout = int(conf.get('response_timeout', 30))
        self.connect_retry = int(conf.get('connect_retry', 3))
        self.device_map = MemcachedDeviceMap(conf, logger)
        self.conn_pool = {}
        		
	def _new_connection(self, device, **kwargs):
        kwargs.setdefault('connect_timeout', self.connect_timeout)
        kwargs.setdefault('response_timeout', self.response_timeout)
        device_info = self.device_map[device]
        for i in range(1, self.connect_retry + 1):
            try:
                return KineticSwiftClient(self.logger, device_info, **kwargs)
            except Timeout:
                self.logger.warning('Drive %s connect timeout #%d (%ds)' % (
                    device, i, self.connect_timeout))
            except Exception:
                self.logger.exception('Drive %s connection error #%d' % (
                    device, i))
            if i < self.connect_retry:
                sleep(1)
        msg = 'Unable to connect to drive %s after %s attempts' % (
            device, i)
        self.logger.error(msg)
        self.faulted_device(device)        
        raise DiskFileDeviceUnavailable()    

    def get_connection(self, device):
        conn = None
        if self.persist_connection:        
            try:
                conn = self.conn_pool[device]
            except KeyError:
                pass
            if conn and conn.faulted:
                conn.close()
                conn = None
            if not conn:
                conn = self.conn_pool[device] = self._new_connection(device)
        else:
            conn = self._new_connection(device)                        
        return conn              
        
    def faulted_device(self, device): 
        del self.device_map[device]
        if self.persist_connection:
            del self.conn_pool[device]    