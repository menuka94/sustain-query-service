package org.sustain.handlers.spark.regression;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.spark.regression.GradientBoostRegressionTask;
import org.sustain.tasks.spark.regression.RegressionTask;

import java.util.List;

public class GradientBoostRegressionHandler extends RegressionHandler {

    private static final Logger log = LogManager.getLogger(GradientBoostRegressionHandler.class);

    public GradientBoostRegressionHandler(ModelRequest request,
                                        StreamObserver<ModelResponse> responseObserver,
                                        SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean hasAppropriateRegressionRequest(ModelRequest modelRequest) {
        return modelRequest.hasGBoostRegressionRequest();
    }

    @Override
    public RegressionTask createRegressionTask(List<String> gisJoinBatch) {
        return new GradientBoostRegressionTask(this.request, gisJoinBatch);
    }
}
