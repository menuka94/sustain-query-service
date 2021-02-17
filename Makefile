
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.EXPORT_ALL_VARIABLES:

SERVER_HOST = lattice-165
SERVER_PORT = 50051
SPARK_MASTER = spark://lattice-165:8079
DB_NAME = sustaindb
DB_USERNAME = ""
DB_PASSWORD = ""
DB_HOST = lattice-46
DB_PORT = 27017

.PHONY: build
build:
	chmod +x ./gradlew
	./gradlew copyDependencies
	./gradlew install -x test

build-with-tests:
	chmod +x ./gradlew
	./gradlew install

run-sustain-server:
	chmod +x ./gradlew
	./gradlew install -x test
	./build/install/sustain-census-grpc/bin/sustain-server

test:
	chmod +x ./gradlew
	./gradlew test

run-spatial-client:
	./build/install/sustain-census-grpc/bin/spatial-client

run-linear-model:
	./build/install/sustain-census-grpc/bin/linear-model

proto:
	chmod +x ./gradlew
	./gradlew generateProto

clean:
	rm -rf build log
