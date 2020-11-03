# ------------------------------------------------------------------------------------*/
# Dockerfile -
# 
# Description: Provides instructions for building an Envoy Docker image.
#   Example found in Envoy Docker documentation: https://www.envoyproxy.io/docs/envoy/latest/start/docker
# 
# Author: Caleb Carlson
# ------------------------------------------------------------------------------------*/

FROM envoyproxy/envoy-alpine:v1.13.1 AS envoy

COPY ./envoy.yaml /etc/envoy/envoy.yaml

RUN chmod go+r /etc/envoy/envoy.yaml
