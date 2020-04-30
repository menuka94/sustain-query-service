#!/bin/bash

rm -rf build;
./gradlew install;
cd build/classes/java/main && java -cp ../../../libs/sustain-census-grpc-1.0-SNAPSHOT.jar org.sustain.census.CensusServer;