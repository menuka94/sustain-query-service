package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.sustain.Collection;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.modeling.SustainLinearRegression;
import org.sustain.util.Constants;

public class RegressionQueryHandler extends GrpcHandler<ModelRequest, ModelResponse> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        if (isValidModelRequest(this.request)) {
            logRequest(this.request);
            LinearRegressionRequest req = this.request.getLinearRegressionRequest();
            for (String gisJoin: req.getGisJoinsList()) {
                LinearRegressionResponse modelResults = buildModel(this.request, gisJoin);
                ModelResponse response = ModelResponse.newBuilder()
                        .setLinearRegressionResponse(modelResults)
                        .build();

                logResponse(response);
                this.responseObserver.onNext(response);
            }
        } else {
            log.warn("Invalid Model Request!");
        }
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
}
