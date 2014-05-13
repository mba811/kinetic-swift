execute "apt-get-update" do
  command "apt-get update && touch /tmp/.apt-get-update"
  creates "/tmp/.apt-get-update"
  action :run
end

required_packages = [
  "default-jdk",
  "maven",
  "protobuf-compiler",
  "libprotobuf-dev",
  "python-distribute",
  "ipython",
  "gunicorn",
  "g++",  # msgpack ext wants this
  "screen",
]
required_packages.each do |pkg|
  package pkg do
    action :install
  end
end

KINETIC_JAR="/vagrant/kinetic-java/target/kinetic-0.5.0.1-SNAPSHOT-jar-with-dependencies.jar"

execute "mvn-package" do
  cwd "/vagrant/kinetic-java"
  command "mvn clean package" 
  creates KINETIC_JAR
end

execute "python-kinetic-install" do
  cwd "/vagrant/kinetic-py"
  command "python setup.py develop"
end

execute "python-kinetic-swift-install" do
  cwd "/vagrant/"
  command "python setup.py develop"
  creates "/usr/local/lib/python2.7/dist-packages/kinetic-swift.egg-link"
end

# build protbuf

PROTOBUF_VERSION = "2.4.1"

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
  cwd "/opt/protobuf-#{PROTOBUF_VERSION}/python/"
  command "python setup.py build && python setup.py install"
  environment "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" => "cpp"
  creates "/usr/local/lib/python2.7/dist-packages/" \
    "protobuf-#{PROTOBUF_VERSION}-py2.7-linux-x86_64.egg/"
end

# setup environment

execute "update-path" do
  command "echo 'export PATH=$PATH:/vagrant/bin' >> /home/vagrant/.profile"
  not_if "grep /vagrant/bin /home/vagrant/.profile"
  action :run
end

{
  "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" => "cpp",
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

[
  "/etc/kinetic",
  "/var/cache/swift/kinetic",
].each do |d|
  directory d do
    owner "vagrant"
    group "vagrant"
    action :create
  end
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
  execute "move-#{i}-base-conf" do
    cwd "/etc/swift/object-server"
    command "mv #{i}.conf #{i}.conf.d/00_base.conf" 
    creates "/etc/swift/object-server/#{i}.conf.d/00_base.conf"
    action :run
  end
  cookbook_file "/etc/swift/object-server/#{i}.conf.d/10_plugin.conf" do
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


