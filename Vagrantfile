Vagrant.configure("2") do |config|
  config.vm.hostname = "kinetic-swift"
  config.vm.box = "kinetic-swift"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"
  config.vm.provider :virtualbox do |vb|
    vb.name = "kinetic-swift-%d" % Time.now
  end
  config.vm.provision :chef_solo do |chef|
    chef.add_recipe "swift"
    chef.add_recipe "kinetic"
  end
end
