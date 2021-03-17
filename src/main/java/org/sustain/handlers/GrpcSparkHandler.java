package org.sustain.handlers;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.sustain.SparkManager;

/**
 * Abstract interface for gRPC handlers.
 * @param <T> A generic for the type of gRPC request.
 * @param <E> A generic for the type of gRPC response.
 */
public abstract class GrpcSparkHandler<T, E> extends GrpcHandler<T, E> {
    protected final SparkManager sparkManager;

    public GrpcSparkHandler(T request, StreamObserver<E> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver);
        this.sparkManager = sparkManager;
    }
}
