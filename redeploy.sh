#!/bin/bash

make clean
cp configurations/config.properties src/main/resources/config.properties
docker build -t inf0rmatiker/sustain:sustain-dev . && docker push inf0rmatiker/sustain:sustain-dev

kubectl delete -f deployment.yaml && kubectl apply -f deployment.yaml

