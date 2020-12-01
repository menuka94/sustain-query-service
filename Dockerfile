# ------------------------------------------------------------------------------------*/
# Dockerfile -
# 
# Description: Provides instructions for building sustain-query-service Docker image.
#
# Author: Caleb Carlson
# ------------------------------------------------------------------------------------*/

FROM node:latest AS base

# --- Dependencies ---

# Install default Java JDK
RUN apt-get update && apt-get install -y default-jdk


# --- Project ---

# Establish environment variables
ENV PROJECT="sustain-query-service" \
    DB_NAME="sustain" \
    DB_PORT=27017

# Add in source code
RUN mkdir -p /code/$PROJECT
WORKDIR /code/$PROJECT

COPY Makefile gradlew gradlew.bat build.gradle settings.gradle ./
COPY nodejs-client/ ./nodejs-client
COPY src/ ./src
COPY bin/ ./bin
COPY gradle/ ./gradle

# Build project
RUN ./gradlew install

ENTRYPOINT ["./bin/sustain-server.sh"]
