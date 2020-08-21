[![Build Status](https://travis-ci.com/Project-Sustain/sustain-census.svg?branch=master)](https://travis-ci.com/Project-Sustain/sustain-census)
[![Maintainability](https://api.codeclimate.com/v1/badges/643c77ef8bf644ea3492/maintainability)](https://codeclimate.com/github/Project-Sustain/sustain-census/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/643c77ef8bf644ea3492/test_coverage)](https://codeclimate.com/github/Project-Sustain/sustain-census/test_coverage)

# Project SUSTAIN 
## gRPC Query Service

### How-to-Run

1. Create a file `src/main/resources/config.properties` using the given sample `src/main/resources/config.properties.sample`.
2. Fill in the values SERVER_HOST=<where you're planning to start the org.sustain.server.SustainServer>, DB_HOST=lattice-0,DB_PORT=27017
3. Do `./gradlew install` from the project root.
4. Run `bin/sustain-server.sh` to start the SustainServer
5. Use `org.sustain.client.SpatialClient` and `src/main/proto/census.proto` as references for implementing clients. An example Node.js client is available at `nodejs-client/census_client.js`

