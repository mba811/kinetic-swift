# to get a plain node with simulators only
# export SIMULATOR_ONLY=true
simulator_only = (ENV['SIMULATOR_ONLY'] || 'false').downcase == 'true'
if simulator_only then
  VAGRANT_ROOT = File.dirname(File.expand_path(__FILE__))
  disk_files = [
    {
      :filename => "disk1.vdi",
      :port => 1,
      :device => 0,
    },
    {
      :filename => "disk2.vdi",
      :port => 2,
      :device => 0,
    },
    {
      :filename => "disk3.vdi",
      :port => 3,
      :device => 0,
    },
  ]
end

Vagrant.configure("2") do |config|
  config.vm.hostname = "kinetic-swift"
  config.vm.box = "ubuntu/trusty64"
  config.vm.network :private_network, ip: "192.168.23.230"
  config.vm.provider :virtualbox do |vb|
    vb.name = "kinetic-swift-%d" % Time.now
    vb.memory = 2048
    if simulator_only then
      disk_files.each do |disk|
        unless File.exist?(disk[:filename])
          vb.customize [
            'createhd',
            '--filename', disk[:filename],
            '--size', 200 * 1024,
          ]
        end
        vb.customize [
          'storageattach', :id,
          '--storagectl', 'SATAController',
          '--port', disk[:port],
          '--device', disk[:device],
          '--type', 'hdd',
          '--medium', disk[:filename],
        ]
      end
    end
  end
  config.vm.provision :chef_solo do |chef|
    if simulator_only then
      chef.add_recipe "swift::disks"
      chef.add_recipe "kinetic::base"
    else
      chef.add_recipe "swift"
      chef.add_recipe "kinetic"
    end
  end
end
