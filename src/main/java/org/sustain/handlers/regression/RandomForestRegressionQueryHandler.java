package org.sustain.handlers.regression;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.handlers.tasks.RandomForestRegressionTask;
import org.sustain.handlers.tasks.RegressionTask;

import java.util.List;

public class RandomForestRegressionQueryHandler extends RegressionQueryHandler {

    private static final Logger log = LogManager.getLogger(RandomForestRegressionQueryHandler.class);

    public RandomForestRegressionQueryHandler(ModelRequest request,
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
