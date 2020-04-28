package org.sustain.census;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.controller.GeoIdResolver;
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
        int port = 50051;   // TODO: Read port from config
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
            server.awaitTermination(30, TimeUnit.SECONDS);
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
            String aspect = request.getAspect();


            // TODO: implement controller selection w.r.t. aspect
            try {
                int resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, aspect);
                int totalPopulation = PopulationController.fetchTotalPopulation(resolutionKey, resolutionValue);

                CensusResponse response = CensusResponse.newBuilder().setResponse(totalPopulation).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (SQLException e) {
                log.error("Error in fetching data");
                e.printStackTrace();
            }
        }
    }
}
