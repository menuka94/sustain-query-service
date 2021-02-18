package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.util.Constants;

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
        for (int i = 0; i < this.request.getCollectionsCount(); i++) {
            Collection col = this.request.getCollections(i);
            log.info("\tName: {}", col.getName());
            log.info("\tLabel: {}", col.getLabel());
            log.info("\tFeatures:");
            for (int j = 0; j < col.getFeaturesCount(); j++) {
                log.info("\t\t{}", col.getFeatures(j));
            }
        }

        log.info("LinearRegressionRequest:");
        LinearRegressionRequest req = this.request.getLinearRegressionRequest();
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

    private LinearRegressionResponse buildModel(ModelRequest modelRequest, String gisJoin) {
        String sparkMaster = Constants.Spark.MASTER;
        String mongoUri = String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT);
        String dbName = Constants.DB.NAME;

        Collection collection = modelRequest.getCollections(0); // We only support 1 collection currently
        SustainLinearRegression model = new SustainLinearRegression(sparkMaster, mongoUri, dbName, collection.getName(),
                gisJoin);

        // Set parameters of Linear Regression Model
        LinearRegressionRequest lrRequest = modelRequest.getLinearRegressionRequest();

        int featuresCount = collection.getFeaturesCount();
        String[] features = new String[featuresCount];
        for (int i = 0; i < featuresCount; i++) {
            features[i] = collection.getFeatures(i);
        }

        model.setFeatures(features);
        model.setLabel(collection.getLabel());
        model.setLoss(lrRequest.getLoss());
        model.setAggregationDepth(lrRequest.getAggregationDepth());
        model.setMaxIterations(lrRequest.getMaxIterations());
        model.setElasticNetParam(lrRequest.getElasticNetParam());
        model.setEpsilon(lrRequest.getEpsilon());
        model.setRegularizationParam(lrRequest.getRegularizationParam());
        model.setConvergenceTolerance(lrRequest.getConvergenceTolerance());
        model.setFitIntercept(lrRequest.getFitIntercept());
        model.setSetStandardization(lrRequest.getSetStandardization());

        model.buildAndRunModel();

        return LinearRegressionResponse.newBuilder()
                .setGisJoin(model.getGisJoin())
                .setTotalIterations(model.getTotalIterations())
                .setRmseResidual(model.getRmse())
                .setR2Residual(model.getR2())
                .setIntercept(model.getIntercept())
                .addAllSlopeCoefficients(model.getCoefficients())
                .addAllObjectiveHistory(model.getObjectiveHistory())
                .build();
    }

    /**
     * Checks the validity of a ModelRequest object, in the context of a Linear Regression request.
     * @param modelRequest The ModelRequest object populated by the gRPC endpoint.
     * @return Boolean true if the model request is valid, false otherwise.
     */
    private boolean isValidModelRequest(ModelRequest modelRequest) {
        if (modelRequest.getType().equals(ModelType.LINEAR_REGRESSION)) {
            if (modelRequest.getCollectionsCount() == 1) {
                if (modelRequest.getCollections(0).getFeaturesCount() == 1) {
                    return modelRequest.hasLinearRegressionRequest();
                }
            }
        }

        return false;
    }

    public void handleQuery() {
        if (isValidModelRequest(this.request)) {
            logRequest();
            LinearRegressionRequest req = this.request.getLinearRegressionRequest();
            for (String gisJoin: req.getGisJoinsList()) {
                LinearRegressionResponse modelResults = buildModel(this.request, gisJoin);
                ModelResponse response = ModelResponse.newBuilder()
                        .setLinearRegressionResponse(modelResults)
                        .build();

                this.responseObserver.onNext(response);
            }
        } else {
            log.warn("Invalid Model Request!");
        }
    }
}
