# ------------------------------------------------------------------------------------*/
# Dockerfile -
# 
# Description: Provides instructions for building sustain-query-service Docker image.
#
# Author: Caleb Carlson
# ------------------------------------------------------------------------------------*/

FROM gradle:7.2.0-jdk11 AS base

# Add in source code
ENV PROJECT="sustain-query-service"
RUN mkdir -p /code/$PROJECT
WORKDIR /code/$PROJECT

# Environment variables
ENV SERVER_PORT=50051
ENV DB_NAME="sustaindb"
ENV DB_HOST="localhost"
ENV DB_PORT=27018
ENV SPARK_MASTER="spark://lattice-100:8079"
ENV SPARK_THREAD_COUNT=4
ENV SPARK_EXECUTOR_CORES=5
ENV SPARK_EXECUTOR_MEMORY="8G"
ENV SPARK_INITIAL_EXECUTORS=5
ENV SPARK_MIN_EXECUTORS=1
ENV SPARK_MAX_EXECUTORS=10
ENV SPARK_BACKLOG_TIMEOUT="10s"
ENV SPARK_IDLE_TIMEOUT="10s"

COPY Makefile gradlew gradlew.bat build.gradle settings.gradle ./
COPY nodejs-client/ ./nodejs-client
COPY src/ ./src
COPY bin/ ./bin
COPY gradle/ ./gradle

# Build project
RUN ./gradlew install

ENTRYPOINT ["./bin/sustain-server.sh", ">", "sustain.log"]
