package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.LinearRegressionTask;
import org.sustain.tasks.RegressionTask;

import java.util.List;

public class LinearRegressionQueryHandler extends RegressionQueryHandler {

    private static final Logger log = LogManager.getLogger(LinearRegressionQueryHandler.class);

    public LinearRegressionQueryHandler(ModelRequest request,
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
