
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.EXPORT_ALL_VARIABLES:

SERVER_HOST = lattice-1
SERVER_PORT = 30051
SPARK_MASTER = spark://lattice-1:32531
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
	./gradlew copyDependencies
	./gradlew install

run-sustain-server:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/sustain-server

test:
	chmod +x ./gradlew
	./gradlew test

run-linear-model:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/linear-model

rforest:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/rforest

gboost:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/gboost

proto:
	chmod +x ./gradlew
	./gradlew generateProto

clean:
	rm -rf build log
