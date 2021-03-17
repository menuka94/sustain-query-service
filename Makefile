
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build

build: executable
	./gradlew copyDependencies
	./gradlew install -x test

server: build
	./build/install/sustain-census-grpc/bin/sustain-server

test: build
	./gradlew test

proto: executable
	./gradlew generateProto

rforest:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/rforest

gboost:
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/gboost

executable:
	chmod +x ./gradlew

clean:
	rm -rf build log
