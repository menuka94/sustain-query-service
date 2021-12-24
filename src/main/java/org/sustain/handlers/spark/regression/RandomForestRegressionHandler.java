package org.sustain.handlers.spark.regression;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.spark.regression.RandomForestRegressionTask;
import org.sustain.tasks.spark.regression.RegressionTask;

import java.util.List;

public class RandomForestRegressionHandler extends RegressionHandler {

    private static final Logger log = LogManager.getLogger(RandomForestRegressionHandler.class);

    public RandomForestRegressionHandler(ModelRequest request,
                                        StreamObserver<ModelResponse> responseObserver,
                                        SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean hasAppropriateRegressionRequest(ModelRequest modelRequest) {
        return modelRequest.hasRForestRegressionRequest();
    }

    @Override
    public RegressionTask createRegressionTask(List<String> gisJoinBatch) {
        return new RandomForestRegressionTask(this.request, gisJoinBatch);
    }
}
