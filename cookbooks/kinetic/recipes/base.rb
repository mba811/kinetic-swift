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

# install package updates

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

# build kinetic-simulator

KINETIC_JAR="/vagrant/kinetic-java/kinetic-simulator/target/kinetic-simulator-0.8.0.4-SNAPSHOT-jar-with-dependencies.jar"

execute "mvn-package" do
  cwd "/vagrant/kinetic-java"
  command "mvn clean package -DSkipTests"
  creates KINETIC_JAR
end


# configure environment

[
  "/etc/kinetic",
  "/var/cache/swift/kinetic",
].each do |d|
  directory d do
    owner "vagrant"
    group "vagrant"
    action :create
    recursive true
  end
end

{
  "KINETIC_JAR" => KINETIC_JAR,
}.each do |var, value|
  execute "kinetic-env-#{var}" do
    command "echo 'export #{var}=#{value}' >> /home/vagrant/.profile"
    not_if "grep #{var} /home/vagrant/.profile"
  end
end

# simulator services

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

