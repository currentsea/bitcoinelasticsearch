# Bitsearch 
## Version: 1.0 BETA

`exchange` - directory containing interactions with the various exchanges

* EACH EXCHANGE DIRECTORY CONTAINS A `private` and a `public` subdirectory to differentiate between requests requiring pre-configuration and those that do not 
** The exchanges that are supported are shown below: 
* `exchange/poloniex` - directory containing poloniex exchange connectors for elasticsearch interacting only with the public
* `exchange/okcoin` - directory containing poloniex exchange connectors for elasticsearch interacting only with the public
* `exchange/bitfinex` - directory containing poloniex exchange connectors for elasticsearch interacting only with the public
* `exchange/kraken` - directory containing poloniex exchange connectors for elasticsearch interacting only with the public

* Please see the `README.md` located in each of the directories for indivdiual instructions (if there is no readme, its probably a good idea to contribute one back!  read the fucking source code if you cant figure it out lol) 

`image` - directory containing source code to build a deployable node of any given connector 
`image/dsl` - dsl directory for jenkins jobs defined using the Jenkins Job DSL Plugin (https://wiki.jenkins-ci.org/display/JENKINS/Job+DSL+Plugin) 
`image/tools` - tools used to build the image on any device 
`image/puppet` - puppet manifest for building a elasticsearch connector 
`image/packer` - packer manifest used to build the deployable
`image/service` - service scripts for each connector type (supporting only Ubuntu 14 as of this release) 
`image/service/init` - contains configuration files to be called in the service scripts located in `image/service/init.d` 
`image/service/init.d` service script for starting and stopping elasticsearch connectors 
