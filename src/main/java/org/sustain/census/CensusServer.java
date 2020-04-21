package org.sustain.census;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class CensusServer {
    private static final Logger log = LogManager.getLogger(CensusServer.class);

    private Server server;

    private void start() throws IOException {
        int port = 50051;   // TODO: Read port from config
        server = ServerBuilder.forPort(port).build().start();
        log.info("Server started, listening on " + port);
    }

    public static void main(String[] args) {

    }

    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void getData(CensusRequest request, StreamObserver<CensusResponse> responseObserver) {

        }
    }
}
