
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build

build: executable
	./gradlew copyDependencies
	./gradlew install -x test

server: build
	./build/install/sustain-query-service/bin/sustain-server

test: build
	./gradlew test

proto: executable
	./gradlew generateProto

rforest: executable
	chmod +x ./gradlew
	./build/install/sustain-query-service/bin/rforest

gboost:executable
	chmod +x ./gradlew
	./build/install/sustain-query-service/bin/gboost

executable:
	chmod +x ./gradlew

clean:
	rm -rf build log
