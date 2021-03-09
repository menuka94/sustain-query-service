package org.sustain.server;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Launches the Sustain Server as a part of the testing JVM process, allowing gRPC methods to be tested.
 */
public class InProcessServer {

    private static final Logger log = LogManager.getLogger(InProcessServer.class);

    private Server server;

    public InProcessServer(){
    }

    public void start() throws IOException {
        server = InProcessServerBuilder
                .forPort(50051)
                .forName("test")
                .addService(new SustainServer.SustainService())
                .addService(new SustainServer.JsonProxyService())
                .build()
                .start();
        log.info("org.sustain.server.InProcessServer started.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            InProcessServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the gRPC library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
