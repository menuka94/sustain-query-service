package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;

public class RegressionQueryHandler {
    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    private void logRequest() {
        log.info("============== REQUEST ===============");

        log.info("Collections:");
        for (int i = 0; i < request.getCollectionsCount(); i++) {
            Collection col = request.getCollections(i);
            log.info("\tName: {}", col.getName());
            log.info("\tLabel: {}", col.getLabel());
            log.info("\tFeatures:");
            for (int j = 0; j < col.getFeaturesCount(); j++) {
                log.info("\t\t{}", col.getFeatures(j));
            }
        }

        log.info("LinearRegressionRequest:");
        LinearRegressionRequest req = request.getLinearRegressionRequest();
        log.info("\tGISJoins:");
        for (int i = 0; i < req.getGisJoinsCount(); i++) {
            log.info("\t\t{}", req.getGisJoins(i));
        }

        log.info("\tLoss: {}", req.getLoss());
        log.info("\tSolver: {}", req.getSolver());
        log.info("\tMaxIterations: {}", req.getMaxIterations());
        log.info("\tAggregationDepth: {}", req.getAggregationDepth());
        log.info("\tElasticNetParam: {}", req.getElasticNetParam());
        log.info("\tEpsilon: {}", req.getEpsilon());
        log.info("\tEpsilon: {}", req.getEpsilon());
        log.info("\tConvergenceTolerance: {}", req.getConvergenceTolerance());
        log.info("\tRegularizationParam: {}", req.getRegularizationParam());
        log.info("\tSetStandardization: {}", req.getSetStandardization());
        log.info("\tFitIntercept: {}", req.getFitIntercept());
        log.info("=======================================");
    }

    public void handleQuery() {
        logRequest();
    }
}
