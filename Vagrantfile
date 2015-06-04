Vagrant.configure("2") do |config|
  config.vm.hostname = "kinetic-swift"
  config.vm.box = "ubuntu/trusty64"
  config.vm.network :private_network, ip: "192.168.23.230"
  config.vm.provider :virtualbox do |vb|
    vb.name = "kinetic-swift-%d" % Time.now
    vb.memory = 2048
  end
  config.vm.provision :chef_solo do |chef|
    chef.add_recipe "swift"
    chef.add_recipe "kinetic"
  end
end
