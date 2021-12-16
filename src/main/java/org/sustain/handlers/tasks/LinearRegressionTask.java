package org.sustain.handlers.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.modeling.SustainLinearRegressionModel;
import org.sustain.modeling.SustainRegressionModel;
import java.util.List;

public class LinearRegressionTask extends RegressionTask {

    private static final Logger log = LogManager.getLogger(LinearRegressionTask.class);

    // Original gRPC request object containing request parameters
    private final LinearRegressionRequest lrRequest;

    public LinearRegressionTask(ModelRequest modelRequest, List<String> gisJoins) {
        this.lrRequest = modelRequest.getLinearRegressionRequest();
        this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
        this.gisJoins = gisJoins;
    }

    @Override
    public SustainRegressionModel buildRegressionModel() {
        return new SustainLinearRegressionModel.LinearRegressionModelBuilder()
                .withLoss(this.lrRequest.getLoss())
                .withSolver(this.lrRequest.getSolver())
                .withAggregationDepth(this.lrRequest.getAggregationDepth())
                .withMaxIterations(this.lrRequest.getMaxIterations())
                .withElasticNetParam(lrRequest.getElasticNetParam())
                .withEpsilon(this.lrRequest.getEpsilon())
                .withRegularizationParam(this.lrRequest.getRegularizationParam())
                .withTolerance(this.lrRequest.getConvergenceTolerance())
                .withFitIntercept(this.lrRequest.getFitIntercept())
                .withStandardization(this.lrRequest.getSetStandardization())
                .build();
    }

    @Override
    public ModelResponse buildModelResponse(String gisJoin, SustainRegressionModel model) {
        if (model instanceof SustainLinearRegressionModel) {
            SustainLinearRegressionModel sustainLinearRegressionModel = (SustainLinearRegressionModel) model;
            LinearRegressionResponse modelResults = LinearRegressionResponse.newBuilder()
                    .setGisJoin(gisJoin)
                    .setTotalIterations(sustainLinearRegressionModel.getTotalIterations())
                    .setRmseResidual(sustainLinearRegressionModel.getRmse())
                    .setR2Residual(sustainLinearRegressionModel.getR2())
                    .setIntercept(sustainLinearRegressionModel.getIntercept())
                    .addAllSlopeCoefficients(sustainLinearRegressionModel.getCoefficients())
                    .addAllObjectiveHistory(sustainLinearRegressionModel.getObjectiveHistory())
                    .build();

            return ModelResponse.newBuilder()
                    .setLinearRegressionResponse(modelResults)
                    .build();
        }
        log.error("Expected model to be of instance LRModel, got {} instead", model.getClass().getSimpleName());
        return null;
    }
}