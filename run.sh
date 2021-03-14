#!/bin/bash

source ./env.sh

function valid_env {
  [ -z ${SERVER_HOST:?"Please set environment variable SERVER_HOST before running! Default: lattice-100"} ]
  [ -z ${SERVER_PORT:?"Please set environment variable SERVER_PORT before running! Default: 50051"} ]
  [ -z ${DB_HOST:?"Please set environment variable DB_HOST before running! Default: lattice-100"} ]
  [ -z ${DB_PORT:?"Please set environment variable DB_PORT before running! Default: 27017"} ]
  [ -z ${DB_NAME:?"Please set environment variable DB_NAME before running! Default: sustaindb"} ]
  [ -z ${SPARK_MASTER:?"Please set environment variable SPARK_MASTER before running! Default: spark://lattice-100:8079"} ]
}

valid_env
make server
