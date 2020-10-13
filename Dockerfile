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

# Install Gradle
#RUN wget https://downloads.gradle-dn.com/distributions/gradle-6.5.1-bin.zip
#RUN unzip gradle-6.5.1-bin.zip && mv gradle-6.5.1 /usr/local/gradle
#ENV PATH="/usr/local/gradle/bin:${PATH}"


# --- Project ---

# Add in source code
ENV PROJECT="sustain-query-service"
RUN mkdir -p /code/$PROJECT
WORKDIR /code/$PROJECT

COPY Makefile gradlew gradlew.bat build.gradle settings.gradle ./
COPY nodejs-client/ ./nodejs-client
COPY src/ ./src
COPY bin/ ./bin
COPY gradle/ ./gradle

# Build project
COPY ./configurations/config.properties ./src/main/resources/config.properties
RUN ./gradlew install

ENTRYPOINT ["./bin/sustain-server.sh"]
