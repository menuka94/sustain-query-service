package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.GradientBoostRegressionTask;
import org.sustain.tasks.RegressionTask;

import java.util.List;

public class GradientBoostRegressionQueryHandler extends RegressionQueryHandler {

    private static final Logger log = LogManager.getLogger(GradientBoostRegressionQueryHandler.class);

    public GradientBoostRegressionQueryHandler(ModelRequest request,
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
