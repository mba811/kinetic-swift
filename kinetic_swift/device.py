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

import memcache 
from swift.common.exceptions import DiskFileDeviceUnavailable

class MemcachedDeviceInfo(object):
    
    def __init__(self, wwn, entry):
        self.wwn = wwn
        self.port = 8123 # default port
        address = entry.split(':')
        self.host = address[0]
        if len(address) > 1: self.port = address[1]
        
class MemcachedDeviceMap(object):
    
    AVAILABLE_PREFIX = 'kinetic/available/'
    MEMCACHE_SERVERS = ['127.0.0.1:11211']       
    
    def __init__(self, conf, logger):         
        self.logger = logger
        self.memcache_client = memcache.Client(MEMCACHE_SERVERS, debug=0)
        self.available_prefix = conf.get('kinetic_available_prefix', AVAILABLE_PREFIX)
    
    def __getitem__(self, device):
        # Caching this will make normal behavior 1ms or 2 faster but 
        # will increase the cost of detecting a fail drive on all 
        # object-servers
        entry = self.memcache_client.get(self.available_prefix + str(device))
        
        if entry is None:
            raise DiskFileDeviceUnavailable()
            
        return MemcachedDeviceInfo(device, entry)
    
    def __delitem__(self, device):
        self.memcache_client.delete(self.available_prefix + str(device))

   