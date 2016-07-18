icecp-module-fork
===============

#### Brief Description
- Module that shall fork an incoming channel into multiple channels based on a specified filter criteria defined in `configuration/config.json`.
- It shall take an incoming channel URI as configuration and expose resulting channels (channel URIs) externally.
- The forked channels are of the format `<incomingChannel URI>/<extracted value from the messageFilter>`

 Ex: If the `message-filter` is `$.sensoridentifier`, `incoming-channel` is `ndn:/test-fork`} and the incoming message looked like: `{"timestamp":..., "sensoridentifier":"sensorId1234"}`
 The resulting forked channel will have the format `ndn:/test-fork/sensorId1234`

#### Attributes

The modules exposes 3 attributes:

1. `message-filter` - Filter criteria for forking
2. `incoming-channel` - Channel on which incoming messages will be published on
3. `forked-channels` - Set conatining the URIs of the newly created forked-channels

Attributes 1 and 2 are defined in `configuration/config.json`.

### Install

Clone this repository and run `mvn install`

### Run

Load this module using the icecp-tools CLI with: (see icecp-tools repo for more information on using the icecp-tools CLI to load modules)

`./icecp-cli load -cmd loadAndStartModules -uri ndn:/intel/node/{hostname} -moduleUri file:///{module path}/target/icecp-module-fork-0.1.2-jar-with-dependencies.jar -configUri file:///{module path}/configuration/config.json -D uri=ndn-lab2.jf.intel.com`

### Documentation

 - [Javadoc](https://github.intel.com/pages/iSPA/icecp-module-fork/)

