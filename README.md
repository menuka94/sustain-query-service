[![Build Status](https://travis-ci.com/Project-Sustain/sustain-census.svg?branch=master)](https://travis-ci.com/Project-Sustain/sustain-census)
[![Maintainability](https://api.codeclimate.com/v1/badges/643c77ef8bf644ea3492/maintainability)](https://codeclimate.com/github/Project-Sustain/sustain-census/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/643c77ef8bf644ea3492/test_coverage)](https://codeclimate.com/github/Project-Sustain/sustain-census/test_coverage)

# Project SUSTAIN 

## Sustain Query Service

Sustain Query Service provides a gRPC server to execute MongoDB queries received from a gRPC client. An example of a gRPC client is provided for testing.
The gRPC server uses a `MongoClient` instance to establish a connection to a mongo instance, running either as a mongos router in the case of a sharded cluster,
or a mongod instance in the case of an unsharded replica set. The connection configuration is derived from `src/main/java/resources/config.properties` file at compile-time.

## Usage

### Set Environment Variables
* Create a new file `env.sh` from `env.sh.example` and update values.
* Execute `run.sh`

To clean the project of any build-generated files:

* `$ make clean`

To compile the project and generate the `build/` directory:

* `$ make`

Once the project is compiled and the `build/` directory has been generated, you can start the gRPC server:

* `$ make server`

### Running a Single Test
`./gradlew test --tests SustainServerTest.testExampleEchoQuery`

### Docker

To build and run a Docker container of this project:

* `$ docker build -t sustain-query-service .`
* `$ docker run -it --name=sustain-query-service -p 50051 sustain-query-service`

### Kubernetes

To build and run this project as a Kubernetes Deployment, connecting to a sharded cluster:

* `$ kubectl apply -f deploy/deployment.yaml`

