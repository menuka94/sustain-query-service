package org.sustain.tasks.spark.regression;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.RForestRegressionRequest;
import org.sustain.RForestRegressionResponse;
import org.sustain.modeling.spark.regression.SustainRandomForestRegressionModel;
import org.sustain.modeling.spark.regression.SustainRegressionModel;

import java.util.List;


public class RandomForestRegressionTask extends RegressionTask {

    private static final Logger log = LogManager.getLogger(RandomForestRegressionTask.class);

    private RForestRegressionRequest rfrRequest;

    public RandomForestRegressionTask(ModelRequest modelRequest, List<String> gisJoins) {
        this.rfrRequest = modelRequest.getRForestRegressionRequest();
        this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
        this.gisJoins = gisJoins;
    }

    @Override
    public SustainRegressionModel buildRegressionModel() {
        return new SustainRandomForestRegressionModel.RFRegressionBuilder()
                .withImpurity(this.rfrRequest.getImpurity())
                .withFeatureSubsetStrategy(this.rfrRequest.getFeatureSubsetStrategy())
                .withMinInstancesPerNode(this.rfrRequest.getMinInstancesPerNode())
                .withNumTrees(this.rfrRequest.getNumTrees())
                .withMaxDepth(this.rfrRequest.getMaxDepth())
                .withMaxBins(this.rfrRequest.getMaxBins())
                .withMinInfoGain(this.rfrRequest.getMinInfoGain())
                .withMinWeightFractionPerNode(this.rfrRequest.getMinWeightFractionPerNode())
                .withSubsamplingRate(this.rfrRequest.getSubsamplingRate())
                .withTrainSplit(this.rfrRequest.getTrainSplit())
                .withIsBootstrap(this.rfrRequest.getIsBootstrap())
                .build();
    }

    @Override
    public ModelResponse buildModelResponse(String gisJoin, SustainRegressionModel model) {
        if (model instanceof SustainRandomForestRegressionModel) {
            SustainRandomForestRegressionModel rfrModel = (SustainRandomForestRegressionModel) model;
            RForestRegressionResponse rfrResponse = RForestRegressionResponse.newBuilder()
                    .setGisJoin(gisJoin)
                    .setRmse(rfrModel.getRmse())
                    .setR2(rfrModel.getR2())
                    .build();

            return ModelResponse.newBuilder()
                    .setRForestRegressionResponse(rfrResponse)
                    .build();
        }
        log.error("Expected model to be of instance LRModel, got {} instead", model.getClass().getSimpleName());
        return null;

    }
}
