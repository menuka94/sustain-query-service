package org.sustain.handlers;

import io.grpc.stub.StreamObserver;

/**
 * Abstract interface for gRPC handlers.
 * @param <T> A generic for the type of gRPC request.
 * @param <E> A generic for the type of gRPC response.
 */
public abstract class GrpcHandler<T, E> {

    private final T request;
    private final StreamObserver<E> responseObserver;

    public GrpcHandler(T request, StreamObserver<E> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    /**
     * Logs the incoming gRPC request fields, as chosen by the implementer.
     * @param request The gRPC request object.
     */
    abstract void logRequest(T request);

    /**
     * Logs an outgoing gRPC response, as chosen by the implementer.
     * @param response The gRPC response object.
     */
    abstract void logResponse(E response);

    /**
     * Processes a gRPC request from start to finish.
     */
    abstract void handleRequest();

}
