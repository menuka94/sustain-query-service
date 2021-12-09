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
ENV DB_HOST="lattice-100"
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

# Copy source code and files
COPY Makefile gradlew gradlew.bat build.gradle settings.gradle ./
COPY nodejs-client/ ./nodejs-client
COPY src/ ./src
COPY gradle/ ./gradle

# Make JAVA_HOME symlink that matches host JAVA_HOME
RUN mkdir -p /usr/lib/jvm && ln -s /opt/java/openjdk /usr/lib/jvm/java-11-openjdk-11.0.13.0.8-3.el8_5.x86_64

# Build project
RUN ./gradlew clean && ./gradlew generateProto && ./gradlew build -x test && ./gradlew install -x test

# Set default container execution entrypoint
ENTRYPOINT ["./build/install/sustain-query-service/bin/sustain-server"]
