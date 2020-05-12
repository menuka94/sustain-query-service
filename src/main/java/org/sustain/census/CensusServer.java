package org.sustain.census;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.controller.GeoIdResolver;
import org.sustain.census.controller.IncomeController;
import org.sustain.census.controller.PopulationController;

import java.io.IOException;
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
            server.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void getData(CensusRequest request, StreamObserver<CensusResponse> responseObserver) {
            String resolutionKey = request.getResolutionKey();
            double latitude = request.getLatitude();
            double longitude = request.getLongitude();
            String feature = request.getFeature();

            try {
                int resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                double returnValue = 0;
                switch (feature) {
                    case Constants.CensusFeatures.TOTAL_POPULATION:
                        returnValue = PopulationController.fetchTotalPopulation(resolutionKey, resolutionValue);
                        break;
                    case Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME:
                        returnValue = IncomeController.fetchMedianHouseholdIncome(resolutionKey, resolutionValue);
                        break;
                }

                CensusResponse response = CensusResponse.newBuilder().setResponse(returnValue).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (SQLException e) {
                log.error("Error in fetching data");
                e.printStackTrace();
            }
        }
    }
}
