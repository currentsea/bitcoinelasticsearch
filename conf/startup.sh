#!/bin/bash 
sudo apt-get -y update 
sudo apt-get -y dist-upgrade
sudo apt-get -y install git 
sudo apt-get -y install python3-pip
pip3 install argparse 
pip3 install requests
pip3 install elasticsearch 
pip3 install pytz
git clone https://github.com/currentsea/bitcoinelasticsearch.git 
sudo cp /root/bitcoinelasticsearch/conf/init/* /etc/init
sudo cp /root/bitcoinelasticsearch/conf/init.d/* /etc/init.d 
sudo chmod 755 /etc/init.d/okcoin-init
sudo chmod 755 /etc/init.d/bitfinex-init
sudo update-rc.d okcoin-init defaults
sudo update-rc.d bitfinex-init defaults

