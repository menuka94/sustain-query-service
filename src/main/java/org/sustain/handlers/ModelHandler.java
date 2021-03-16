package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;

/**
 * Abstract interface for gRPC model handlers.
 */
public abstract class ModelHandler extends GrpcHandler<ModelRequest, ModelResponse> {

    protected final JavaSparkContext sparkContext;

    public ModelHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, JavaSparkContext sparkContext) {
        super(request, responseObserver);
        this.sparkContext = sparkContext;
    }

    /**
     * Checks the validity of a ModelRequest object, in the context of a Linear Regression request.
     * @param modelRequest The ModelRequest object populated by the gRPC endpoint.
     * @return Boolean true if the model request is valid, false otherwise.
     */
    abstract boolean isValid(ModelRequest modelRequest);

}
