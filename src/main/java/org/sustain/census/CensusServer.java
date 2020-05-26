package org.sustain.census;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.controller.AgeController;
import org.sustain.census.controller.GeoIdResolver;
import org.sustain.census.controller.IncomeController;
import org.sustain.census.controller.PopulationController;
import org.sustain.census.controller.PovertyController;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class CensusServer {
    private static final Logger log = LogManager.getLogger(CensusServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CensusServer server = new CensusServer();
        server.start();
        server.blockUntilShutdown();
    }

    private void start() throws IOException {
        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
                .addService(new CensusServerImpl())
                .build().start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    CensusServer.this.stop();
                } catch (InterruptedException e) {
                    log.error("Error in stopping the server");
                    e.printStackTrace();
                }
                log.warn("Server is shutting down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void getTotalPopulation(TotalPopulationRequest request,
                                       StreamObserver<TotalPopulationResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getLongitude();
            Decade _decade = request.getSpatialTemporalInfo().getDecade();
            String decade = Constants.DECADES.get(_decade);

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchTotalPopulation(resolutionKey,
                        resolutionValue.intValue(), decade));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getMedianAge(MedianAgeRequest request, StreamObserver<MedianAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(AgeController.fetchMedianAge(resolutionKey, resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getMedianHouseholdIncome(MedianHouseholdIncomeRequest request,
                                             StreamObserver<MedianHouseholdIncomeResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(IncomeController.fetchMedianHouseholdIncome(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getPopulationByAge(PopulationByAgeRequest request,
                                       StreamObserver<PopulationByAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchPopulationByAge(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getPoverty(PovertyRequest request, StreamObserver<PovertyResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PovertyController.fetchPovertyData(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        /**
         * Execute a TargetedQuery - return geographical areas that satisfy a given value range of a census feature
         * Example 1: Retrieve all states where (population >= 1,000,000)
         * Example 2: Retrieve all counties where (median household income < $50,000/year)
         */
        @Override
        public void executeTargetedQuery(TargetQueryRequest request,
                                         StreamObserver<TargetQueryResponse> responseObserver) {
            Predicate predicate = request.getPredicate();
            String comparisonOp = Constants.COMPARISON_OPS.get(predicate.getComparisonOp());
            double comparisonValue = predicate.getComparisonValue();
            Predicate.Feature feature = predicate.getFeature();
            Decade _decade = predicate.getDecade();
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getResolution());

            String decade = Constants.DECADES.get(_decade);

            try {
                switch (feature) {
                    case Population:
                        HashMap<String, String> targetedPopulationResults =
                                PopulationController.fetchTargetedInfo(decade,
                                        resolution, comparisonOp, comparisonValue);

                        TargetQueryResponse.Builder responseBuilder = TargetQueryResponse.newBuilder();

                        // iterator over results, create SpatialInfo objects, attach to responseBuilder
                        for (String key : targetedPopulationResults.keySet()) {
                            TargetQueryResponse.SpatialInfo spatialInfo = TargetQueryResponse.SpatialInfo.newBuilder()
                                    .setGeoid(Integer.parseInt(key))
                                    .setName(targetedPopulationResults.get(key))
                                    .build();
                            responseBuilder.addSpatialInfo(spatialInfo);
                        }

                        TargetQueryResponse response = responseBuilder.build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        break;
                    case Income:
                        IncomeController.fetchTargetedInfo(decade, resolution, comparisonOp, comparisonValue);
                        break;
                    case UNRECOGNIZED:
                        log.warn("Invalid Census feature found in the request");
                        break;
                }
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }
    }
}
