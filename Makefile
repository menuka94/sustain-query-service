
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

run-sustain-server:
	./gradlew install -x test
	sh ./build/install/sustain-census-grpc/bin/sustain-server

test:
	./gradlew test

run-spatial-client:
	sh ./build/install/sustain-census-grpc/bin/spatial-client


proto:
	./gradlew generateProto


clean:
	rm -rf build