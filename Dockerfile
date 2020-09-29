# ------------------------------------------------------------------------------------*/
# Dockerfile -
# 
# Description: Provides instructions for building sustain-query-service Docker image.
#
# Author: Caleb Carlson
# ------------------------------------------------------------------------------------*/

FROM node:latest AS base

# Install default Java JDK
RUN apt-get update && apt-get install -y default-jdk

# Install Gradle
RUN wget https://downloads.gradle-dn.com/distributions/gradle-6.5.1-bin.zip
RUN unzip gradle-6.5.1-bin.zip && mv gradle-6.5.1 /usr/local/gradle
ENV PATH="/usr/local/gradle/bin:${PATH}"


ENV PROJECT="sustain-query-service"
RUN mkdir -p /code/$PROJECT/
COPY .* /code/$PROJECT/
WORKDIR /code/$PROJECT

ENTRYPOINT ["/bin/sh"]
