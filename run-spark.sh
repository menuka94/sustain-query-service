#!/bin/bash

spark-submit --class org.sustain.dataModeling.Main --supervise build/libs/sustain-census-grpc-1.0-SNAPSHOT.jar
