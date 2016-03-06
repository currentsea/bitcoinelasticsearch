file { "/home/ubuntu/bitcoinelasticsearch/conf/init/poloinex-init.init": 
	ensure => "present", 
	before => File["/etc/init/poloinex-init.init"],
}

file { "/etc/init/poloinex-init.init": 
	ensure => "present", 
	owner => "root", 
	mode => "644", 
	source => "/home/ubuntu/bitcoinelasticsearch/conf/init/poloinex-init.init", 
} 

file { "/home/ubuntu/bitcoinelasticsearch/conf/init.d/poloinex-init": 
	ensure => "present", 
	before => File["/etc/init.d/poloinex-init"],
}

file { "/etc/init.d/poloinex-init": 
	ensure => "present", 
	owner => "root", 
	mode => "755",
	source => "/home/ubuntu/bitcoinelasticsearch/conf/init.d/poloinex-init", 
 } 