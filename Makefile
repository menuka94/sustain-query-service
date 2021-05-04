
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

rforest: executable
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/rforest

gboost:executable
	chmod +x ./gradlew
	./build/install/sustain-census-grpc/bin/gboost

executable:
	chmod +x ./gradlew

clean:
	rm -rf build log

test-pca:
	./gradlew test --tests SustainServerTest.testPCAHandler -DCOLLECTION_NAME=$(COLLECTION_NAME)

test-kmeans:
	./gradlew test --tests SustainServerTest.testKMeansClusteringModel -DCOLLECTION_NAME=$(COLLECTION_NAME)

test-bisecting-kmeans:
	./gradlew test --tests SustainServerTest.testBisectingKMeansClusteringModel -DCOLLECTION_NAME=$(COLLECTION_NAME)

test-gaussian-mixture:
	./gradlew test --tests SustainServerTest.testGaussianMixtureClusteringModel -DCOLLECTION_NAME=$(COLLECTION_NAME)

test-lda:
	./gradlew test --tests SustainServerTest.testLDAClusteringModel -DCOLLECTION_NAME=$(COLLECTION_NAME)
