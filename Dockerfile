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

# Copy source code and files
COPY Makefile gradlew gradlew.bat build.gradle settings.gradle ./
COPY nodejs-client/ ./nodejs-client
COPY src/ ./src
COPY gradle/ ./gradle

# Build project
RUN ./gradlew clean && ./gradlew generateProto && ./gradlew build -x test && ./gradlew install -x test

# Set default container execution entrypoint
ENTRYPOINT ["./build/install/sustain-query-service/bin/sustain-server"]
