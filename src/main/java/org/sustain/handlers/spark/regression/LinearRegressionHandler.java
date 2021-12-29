package org.sustain.handlers.spark.regression;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.spark.regression.LinearRegressionTask;
import org.sustain.tasks.spark.regression.RegressionTask;

import java.util.List;

public class LinearRegressionHandler extends RegressionHandler {

    private static final Logger log = LogManager.getLogger(LinearRegressionHandler.class);

    public LinearRegressionHandler(ModelRequest request,
                                        StreamObserver<ModelResponse> responseObserver,
                                        SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean hasAppropriateRegressionRequest(ModelRequest modelRequest) {
        return modelRequest.hasLinearRegressionRequest();
    }

    @Override
    public RegressionTask createRegressionTask(List<String> gisJoinBatch) {
        return new LinearRegressionTask(this.request, gisJoinBatch);
    }

}
