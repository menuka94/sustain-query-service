#!/bin/bash

# Clean up containers
docker kill mongos-router;  docker rm mongos-router
docker kill sustain-dev;    docker rm sustain-dev 

make clean
cp configurations/config.properties src/main/resources/config.properties
make

echo -e ">>> Building sustain-query-service Docker image\n"

docker build -t inf0rmatiker/sustain:sustain-dev .    && \
  echo -e ">>> Uploading Docker image to Dockerhub\n" && \
  docker push inf0rmatiker/sustain:sustain-dev

echo -e ">>> Running mongos router in Docker container\n"
docker run -dit --name="mongos-router" -p 27017:27017 mongo:4.2.8 mongos --configdb cfgrs/lattice-168.cs.colostate.edu:27017,lattice-169.cs.colostate.edu:27017,lattice-170.cs.colostate.edu:27017 --bind_ip_all --port 27017

echo -e ">>> Running sustain-dev in Docker container\n"
docker run -it --name="sustain-dev" -p 50051:50051 inf0rmatiker/sustain:sustain-dev
