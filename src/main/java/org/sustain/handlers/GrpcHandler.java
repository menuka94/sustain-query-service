package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract interface for gRPC handlers.
 * @param <T> A generic for the type of gRPC request.
 * @param <E> A generic for the type of gRPC response.
 */
public abstract class GrpcHandler<T, E> {

    private static final Logger log = LogManager.getLogger(GrpcHandler.class);

    protected final T request;
    protected final StreamObserver<E> responseObserver;

    public GrpcHandler(T request, StreamObserver<E> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    /**
     * Logs the incoming gRPC request fields, as chosen by the implementer.
     * @param request The gRPC request object.
     */
    public void logRequest(T request) {
        log.info("\n\n--- {} ---\n{}", request.getClass().getName(), request.toString());
    }

    /**
     * Logs an outgoing gRPC response, as chosen by the implementer.
     * @param response The gRPC response object.
     */
    public void logResponse(E response) {
        log.info("\n\n--- {} ---\n{}", response.getClass().getName(), response.toString());
    }

    /**
     * Processes a gRPC request from start to finish.
     */
    public abstract void handleRequest();

}
