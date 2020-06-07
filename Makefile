
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build
build:
	chmod +x gradlew
	./gradlew install -x test

build-with-tests:
	chmod +x gradlew
	./gradlew install

run-census-server:
	./gradlew install -x test
	sh ./build/install/sustain-census-grpc/bin/census-server

test:
	./gradlew test

run-census-client:
	sh ./build/install/sustain-census-grpc/bin/census-client


proto:
	./gradlew generateProto


clean:
	rm -rf build