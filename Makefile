
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build
build:
	./gradlew install

.PHONY: run-census-server
run-census-server:
	sh ./build/install/sustain-census-grpc/bin/census-server

