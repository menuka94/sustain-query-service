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
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.sustain.census.ServerHelper.executeTargetedIncomeRequest;
import static org.sustain.census.ServerHelper.executeTargetedPopulationQuery;
import static org.sustain.census.ServerHelper.executeTargetedRaceRequest;


public class CensusServer {
    private static final Logger log = LogManager.getLogger(CensusServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CensusServer server = new CensusServer();
        server.start();
        server.blockUntilShutdown();
    }

    public void start() throws IOException {
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

    public void stop() throws InterruptedException {
        if (server != null) {
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdownNow() {
        if (server != null) {
            server.shutdownNow();
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

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchTotalPopulation(resolutionKey, resolutionValue,
                        decade));
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

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(AgeController.fetchMedianAge(resolutionKey, resolutionValue));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getMedianHouseholdIncome(MedianHouseholdIncomeRequest request,
                                             StreamObserver<MedianHouseholdIncomeResponse> responseObserver) {
            SpatialTemporalInfo spatialTemporalInfo = request.getSpatialTemporalInfo();
            String resolutionKey = spatialTemporalInfo.getResolution();
            double latitude = spatialTemporalInfo.getLatitude();
            double longitude = spatialTemporalInfo.getLongitude();
            String decade = Constants.DECADES.get(spatialTemporalInfo.getDecade());

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(IncomeController.fetchMedianHouseholdIncome(resolutionKey,
                        resolutionValue, decade));
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

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchPopulationByAge(resolutionKey, resolutionValue));
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

            String resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PovertyController.fetchPovertyData(resolutionKey, resolutionValue));
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
        public void executeTargetedQuery(TargetedQueryRequest request,
                                         StreamObserver<TargetedQueryResponse> responseObserver) {
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
                        executeTargetedPopulationQuery(responseObserver, comparisonOp, comparisonValue,
                                resolution, decade);
                        break;
                    case Income:
                        executeTargetedIncomeRequest(responseObserver, comparisonOp, comparisonValue,
                                resolution, decade);
                        break;
                    case Race:
                        executeTargetedRaceRequest(responseObserver, Constants.EMPTY_COMPARISON_FIELD, comparisonOp,
                                comparisonValue,
                                resolution, decade);
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
