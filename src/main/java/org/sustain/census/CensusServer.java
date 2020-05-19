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

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
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
            String resolutionKey = request.getSpatialInfo().getResolution();
            double latitude = request.getSpatialInfo().getLatitude();
            double longitude = request.getSpatialInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchTotalPopulation(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
            }
        }

        @Override
        public void getMedianAge(MedianAgeRequest request, StreamObserver<MedianAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialInfo().getResolution();
            double latitude = request.getSpatialInfo().getLatitude();
            double longitude = request.getSpatialInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(AgeController.fetchMedianAge(resolutionKey, resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
            }
        }

        @Override
        public void getMedianHouseholdIncome(MedianHouseholdIncomeRequest request,
                                             StreamObserver<MedianHouseholdIncomeResponse> responseObserver) {
            String resolutionKey = request.getSpatialInfo().getResolution();
            double latitude = request.getSpatialInfo().getLatitude();
            double longitude = request.getSpatialInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(IncomeController.fetchMedianHouseholdIncome(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
            }
        }

        @Override
        public void getPopulationByAge(PopulationByAgeRequest request,
                                       StreamObserver<PopulationByAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialInfo().getResolution();
            double latitude = request.getSpatialInfo().getLatitude();
            double longitude = request.getSpatialInfo().getLongitude();

            BigInteger resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchPopulationByAge(resolutionKey,
                        resolutionValue.intValue()));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
            }
        }
    }
}
