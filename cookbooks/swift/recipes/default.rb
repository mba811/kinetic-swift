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

include_recipe "swift::disks"

execute "clean-up" do
  command "rm /home/vagrant/postinstall.sh || true"
end

# packages

cookbook_file "/etc/apt/sources.list.d/backports.list" do
  source "etc/apt/sources.list.d/backports.list"
end

execute "apt-get-update" do
  command "apt-get update && touch /tmp/.apt-get-update"
  creates "/tmp/.apt-get-update"
  action :run
end

execute "apt-get-install-fix-swift" do
  command "apt-get -f install -y"
  action :run
end

required_packages = [
  "curl", "gcc", "memcached", "rsync", "sqlite3", "git-core",
  "python-setuptools", "python-coverage", "python-dev", "python-nose",
  "python-simplejson", "python-xattr",  "python-eventlet", "python-greenlet",
  "python-pastedeploy", "python-netifaces", "python-dnspython", "python-mock",
  "libxml2-dev", "libxslt1-dev", "liberasurecode-dev"
]
required_packages.each do |pkg|
  package pkg do
    action :install
  end
end

# run dirs

[
  "/var/run/swift",
].each do |d|
  directory d do
    owner "vagrant"
    group "vagrant"
    action :create
  end
end

(1..4).each do |i|
  recon_cache_path = "/var/cache/swift/node#{i}"
  directory recon_cache_path do
    owner "vagrant"
    group "vagrant"
    recursive true
  end
end

# rsync

cookbook_file "/etc/rsyncd.conf" do
  source "etc/rsyncd.conf"
end

execute "enable-rsync" do
  command "sed -i 's/ENABLE=false/ENABLE=true/' /etc/default/rsync"
  not_if "grep ENABLE=true /etc/default/rsync"
  action :run
end

service "rsync" do
  action :start
end

# python install

bash "fix-git-relative-submodules-swift" do
  cwd "/vagrant"
  code <<-EOF
  for path in $(find ./.git/modules -name config); do
    back_depth=$(dirname $(dirname $path | sed 's|[^/]*/|../|g'))
    sed -i "s|worktree = /vagrant|worktree = ${back_depth}|g" $path
  done
  rm */.git
  git submodule update
  EOF
  not_if 'cd /vagrant && git status'
end

execute "pbr-pip-depends" do
  command "easy_install pip && pip install --upgrade pip setuptools " \
    "&& apt-get purge python-setuptools -y"
end

execute "fix-pbr-testtools-version-depends" do
  command "pip install --upgrade testtools"
end

execute "python-swiftclient-install" do
  cwd "/vagrant/python-swiftclient"
  command "pip install -e . && pip install -r test-requirements.txt"
  # creates "/usr/local/lib/python2.7/dist-packages/python-swiftclient.egg-link"
  action :run
end

execute "python-swift-install" do
  cwd "/vagrant/swift"
  command "pip install -e . && pip install -r test-requirements.txt"
  # creates "/usr/local/lib/python2.7/dist-packages/swift.egg-link"
  action :run
end

# configs
directory "/etc/swift" do
  owner "vagrant"
  group "vagrant"
  action :create
end

cookbook_file "/etc/swift/swift.conf" do
  source "etc/swift/swift.conf"
  owner "vagrant"
  group "vagrant"
end

cookbook_file "/etc/swift/proxy-server.conf" do
  source "etc/swift/proxy-server.conf"
  owner "vagrant"
  group "vagrant"
end


["container", "account"].each_with_index do |server, p|
  directory "/etc/swift/#{server}-server" do
    owner "vagrant"
    group "vagrant"
    action :delete
    recursive true
  end
  directory "/etc/swift/#{server}-server" do
    owner "vagrant"
    group "vagrant"
    action :create
  end
  (1..4).each do |i|
    template "/etc/swift/#{server}-server/#{i}.conf" do
      source "etc/swift/#{server}-server.conf.erb"
      owner "vagrant"
      group "vagrant"
      variables({
         :srv_path => "/srv/node#{i}",
         :bind_port => "60#{i}#{p + 1}",
         :recon_cache_path => "/var/cache/swift/node#{i}",
      })
    end
  end
end

directory "/etc/swift/object-server" do
  owner "vagrant"
  group "vagrant"
  action :create
end

cookbook_file "/etc/swift/object-server/base.conf-template" do
  source "etc/swift/object-server/base.conf-template"
  owner "vagrant"
  group "vagrant"
end

(1..4).each do |i|
  directory "/etc/swift/object-server/#{i}.conf.d" do
    owner "vagrant"
    group "vagrant"
    action :create
  end
  link "/etc/swift/object-server/#{i}.conf.d/00_base.conf" do
    to "/etc/swift/object-server/base.conf-template"
  end
  template "/etc/swift/object-server/#{i}.conf.d/10_default.conf" do
    source "etc/swift/object-server.conf.erb"
    owner "vagrant"
    group "vagrant"
    variables({
       :srv_path => "/srv/node#{i}",
       :bind_port => "60#{i}0",
       :recon_cache_path => "/var/cache/swift/node#{i}",
    })
  end
end

cookbook_file "/etc/swift/test.conf" do
  source "etc/swift/test.conf"
  owner "vagrant"
  group "vagrant"
end

# rings

ring_settings = {
  "account" => {
    "replicas" => 3,
    "port" => 2,
  },
  "container" => {
    "replicas" => 3,
    "port" => 1,
  },
  "object" => {
    "replicas" => 3,
    "port" => 0,
  },
  "object-1" => {
    "replicas" => 6,
    "port" => 0,
  },
}
ring_settings.each do |server, info|
  execute "#{server}.builder-create" do
    command "sudo -u vagrant swift-ring-builder #{server}.builder create " \
      "10 #{info["replicas"]} 1"
    creates "/etc/swift/#{server}.builder"
    cwd "/etc/swift"
  end
  (1..4).each do |i|
    [i, i + 4].each do |d|
      dev = "sdb#{d}"
      execute "#{server}.builder-add-#{dev}" do
        command "sudo -u vagrant swift-ring-builder #{server}.builder add " \
          "r1z#{i}-127.0.0.1:60#{i}#{info["port"]}/#{dev} 1 && " \
          "rm -f /etc/swift/#{server}.ring.gz || true"
        not_if "swift-ring-builder /etc/swift/#{server}.builder search /#{dev}"
        cwd "/etc/swift"
      end
    end
  end
  execute "#{server}.builder-rebalance" do
    command "sudo -u vagrant swift-ring-builder #{server}.builder write_ring"
    not_if "sudo -u vagrant swift-ring-builder /etc/swift/#{server}.builder rebalance"
    creates "/etc/swift/#{server}.ring.gz"
    cwd "/etc/swift"
  end
end

# start main

execute "startmain" do
  command "sudo -u vagrant swift-init start main"
end

# swift command line env setup

{
  "ST_AUTH" => "http://localhost:8080/auth/v1.0",
  "ST_USER" => "test:tester",
  "ST_KEY" => "testing",
}.each do |var, value|
  execute "swift-env-#{var}" do
    command "echo 'export #{var}=#{value}' >> /home/vagrant/.profile"
    not_if "grep #{var} /home/vagrant/.profile"
    action :run
  end
end
