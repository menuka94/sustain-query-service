package org.sustain.tasks.spark.regression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.modeling.spark.regression.SustainGradientBoostRegressionModel;
import org.sustain.modeling.spark.regression.SustainRegressionModel;

import java.util.List;

public class GradientBoostRegressionTask extends RegressionTask {

    private static final Logger log = LogManager.getLogger(GradientBoostRegressionTask.class);

    // Original gRPC request object containing request parameters
    private final GBoostRegressionRequest gbrRequest;

    public GradientBoostRegressionTask(ModelRequest modelRequest, List<String> gisJoins) {
        this.gbrRequest = modelRequest.getGBoostRegressionRequest();
        this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
        this.gisJoins = gisJoins;
    }

    @Override
    public SustainRegressionModel buildRegressionModel() {
        return new SustainGradientBoostRegressionModel.GradientBoostRegressionModelBuilder()
                .withLossType(this.gbrRequest.getLossType())
                .withImpurity(this.gbrRequest.getImpurity())
                .withFeatureSubsetStrategy(this.gbrRequest.getFeatureSubsetStrategy())
                .withMinInstancesPerNode(this.gbrRequest.getMinInstancesPerNode())
                .withMaxDepth(this.gbrRequest.getMaxDepth())
                .withMaxIterations(this.gbrRequest.getMaxIter())
                .withMaxBins(this.gbrRequest.getMaxBins())
                .withMinInfoGain(this.gbrRequest.getMinInfoGain())
                .withMinWeightFractionPerNode(this.gbrRequest.getMinWeightFractionPerNode())
                .withSubsamplingRate(this.gbrRequest.getSubsamplingRate())
                .withStepSize(this.gbrRequest.getStepSize())
                .build();
    }

    @Override
    public ModelResponse buildModelResponse(String gisJoin, SustainRegressionModel model) {
        if (model instanceof SustainGradientBoostRegressionModel) {
            SustainGradientBoostRegressionModel gbrModel = (SustainGradientBoostRegressionModel) model;
            GBoostRegressionResponse modelResults = GBoostRegressionResponse.newBuilder()
                    .setGisJoin(gisJoin)
                    .setRmse(gbrModel.getRmse())
                    .setR2(gbrModel.getR2())
                    .build();

            return ModelResponse.newBuilder()
                    .setGBoostRegressionResponse(modelResults)
                    .build();
        }
        log.error("Expected model to be of instance LRModel, got {} instead", model.getClass().getSimpleName());
        return null;
    }

}
