
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.EXPORT_ALL_VARIABLES:

SERVER_HOST = localhost
SERVER_PORT = 50051
SPARK_MASTER = spark://menuka-HP:7077
DB_NAME = sustaindb
DB_USERNAME = ""
DB_PASSWORD = ""
DB_HOST = localhost
DB_PORT = 27017

.PHONY: build

build:
	chmod +x ./gradlew
	./gradlew copyDependencies
	./gradlew install -x test

build-with-tests:
	chmod +x ./gradlew
	./gradlew copyDependencies
	./gradlew install

run-sustain-server:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/sustain-server

test:
	chmod +x ./gradlew
	./gradlew test

run-spatial-client:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/spatial-client

run-linear-model:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/linear-model

proto:
	chmod +x ./gradlew
	./gradlew generateProto

clean:
	rm -rf build log
