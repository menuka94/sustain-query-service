package org.sustain.handlers.spark;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.sustain.SparkManager;
import org.sustain.handlers.GrpcHandler;

/**
 * Abstract interface for gRPC handlers.
 * @param <T> A generic for the type of gRPC request.
 * @param <E> A generic for the type of gRPC response.
 */
public abstract class SparkHandler<T, E> extends GrpcHandler<T, E> {
    protected final SparkManager sparkManager;

    public SparkHandler(T request, StreamObserver<E> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver);
        this.sparkManager = sparkManager;
    }
}
