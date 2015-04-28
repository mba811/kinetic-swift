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

execute "apt-get-update" do
  command "apt-get update && touch /tmp/.apt-get-update"
  creates "/tmp/.apt-get-update"
  action :run
end

execute "apt-get-install-fix-kinetic" do
  command "apt-get -f install -y"
  action :run
end

required_packages = [
  "build-essential",
  "java7-jdk",
  "maven",
  "libprotobuf-dev",
  "g++",  # msgpack ext wants this
  "screen",
  "ipython",
]
required_packages.each do |pkg|
  package pkg do
    action :install
  end
end

# build protbuf

PROTOBUF_VERSION = "2.5.0"

execute "download-protobuf" do
  cwd "/opt/"
  command "wget https://protobuf.googlecode.com/files/" \
    "protobuf-#{PROTOBUF_VERSION}.tar.gz"
  creates "/opt/protobuf-#{PROTOBUF_VERSION}.tar.gz"
end

execute "extract-protobuf" do
  cwd "/opt/"
  command "tar zxvf protobuf-#{PROTOBUF_VERSION}.tar.gz"
  creates "/opt/protobuf-#{PROTOBUF_VERSION}"
end

execute "build-protobuf" do
  cwd "/opt/protobuf-#{PROTOBUF_VERSION}"
  command "sh ./configure --prefix=/usr && make && make install"
  creates "/usr/bin/protoc"
end

execute "install-python-protobuf" do
  cwd "/opt/protobuf-#{PROTOBUF_VERSION}/python/"
  command "python setup.py build && pip install -e ."
  environment "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" => "cpp"
  creates "/usr/local/lib/python2.7/dist-packages/" \
    "protobuf-#{PROTOBUF_VERSION}-py2.7-linux-x86_64.egg/"
end

# build kinetic-simulator

KINETIC_JAR="/vagrant/kinetic-java/kinetic-simulator/target/kinetic-simulator-0.8.0.3-SNAPSHOT-jar-with-dependencies.jar"

execute "sync-kinetic-proto" do
  cwd "/vagrant/kinetic-java"
  command "./bin/syncProtoFromRepo.sh"
end

execute "build-kinetic-proto" do
  cwd "/vagrant/kinetic-java"
  command "./bin/buildProto.sh"
end

execute "mvn-package" do
  cwd "/vagrant/kinetic-java"
  command "mvn clean package -DSkipTests"
  creates KINETIC_JAR
end

# install kinetic-py

execute "python-submodule-update" do
  cwd "/vagrant/kinetic-py"
  command "git submodule init && git submodule update"
end

bash "fix-git-relative-submodules-kinetic" do
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

execute "python-protoc-build" do
  cwd "/vagrant/kinetic-py"
  command "./compile_proto.sh"
end

execute "python-kinetic-install" do
  cwd "/vagrant/kinetic-py"
  command "pip install -r requirements.txt && pip install -e ."
end

# install kinetic-swift plugin

execute "python-kinetic-swift-install" do
  cwd "/vagrant/"
  command "pip install -e ."
  creates "/usr/local/lib/python2.7/dist-packages/kinetic-swift.egg-link"
end

# setup environment

execute "update-path" do
  command "echo 'export PATH=$PATH:/vagrant/bin' >> /home/vagrant/.profile"
  not_if "grep /vagrant/bin /home/vagrant/.profile"
  action :run
end

[
  "/etc/kinetic",
  "/var/cache/swift/kinetic",
  "/home/vagrant/.python-eggs",
].each do |d|
  directory d do
    owner "vagrant"
    group "vagrant"
    action :create
  end
end

execute "setup-python-eggs" do
  command "python -c 'import kinetic_swift.obj.server'"
  action :run
end

{
  "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" => "cpp",
  "PYTHON_EGG_CACHE" => "/home/vagrant/.python-eggs",
  "KINETIC_JAR" => KINETIC_JAR,
}.each do |var, value|
  execute "kinetic-env-#{var}" do
    command "echo 'export #{var}=#{value}' >> /home/vagrant/.profile"
    not_if "grep #{var} /home/vagrant/.profile"
  end
end

execute "shutdown-swift-object-server" do
  command "swift-init object-server stop || true"
  action :run
end

template "/etc/kinetic/kinetic-simulator.screenrc" do
  source "etc/kinetic/kinetic-simulator.screenrc.erb"
  owner "vagrant"
  group "vagrant"
  variables({
     :kinetic_jar => KINETIC_JAR,
  })
end

execute "simulator-start" do
  command "sudo -i -u vagrant /vagrant/bin/kinetic-simulator start"
  action :run
end

# kinetic ring

execute "kinetic.builder-create" do
  command "sudo -u vagrant swift-ring-builder kinetic.builder create 10 3 1"
  creates "/etc/swift/kinetic.builder"
  cwd "/etc/swift"
end

(1..4).each do |i|
  execute "kinetic-builder-add-80#{i}0" do
    command "sudo -u vagrant swift-ring-builder kinetic.builder add " \
      "--region 1 --zone 1 --ip 127.0.0.1 --replication-ip 127.0.0.1 " \
      "--port 60#{i}0 --replication-port 60#{i}0 --weight 1 " \
      "--device 127.0.0.1:80#{i}0"
      not_if "swift-ring-builder /etc/swift/kinetic.builder search " \
        "/127.0.0.1:80#{i}0"
      cwd "/etc/swift"
  end
  directory "/etc/swift/object-server/#{i}.conf.d" do
    owner "vagrant"
    group "vagrant"
    action :create
  end
  cookbook_file "/etc/swift/object-server/#{i}.conf.d/20_plugin.conf" do
    source "etc/swift/object-server.conf"
    owner "vagrant"
    group "vagrant"
  end
end

cookbook_file "/etc/swift/kinetic.conf" do
  source "etc/swift/kinetic.conf"
  owner "vagrant"
  group "vagrant"
end

execute "kinetic.builder-rebalance" do
  command "sudo -u vagrant swift-ring-builder kinetic.builder rebalance"
  creates "/etc/swift/kinetic.ring.gz"
  cwd "/etc/swift"
end

execute "kinetic.ring.gz-symlink" do
  command "rm object.ring.gz ; sudo -u vagrant ln -s kinetic.ring.gz object.ring.gz"
  not_if "readlink object.ring.gz"
  cwd "/etc/swift"
end

execute "kinetic-object-start" do
  command "sudo -i -u vagrant swift-init object start"
  action :run
end


