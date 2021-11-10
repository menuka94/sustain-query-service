package org.sustain.handlers.tasks;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.*;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.modeling.LRModel;
import org.sustain.modeling.RegressionModel;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public RegressionModel buildRegressionModel() {
        return new LRModel.LRModelBuilder()
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
    public ModelResponse buildModelResponse(String gisJoin, RegressionModel model) {
        if (model instanceof LRModel) {
            LRModel lrModel = (LRModel) model;
            LinearRegressionResponse modelResults = LinearRegressionResponse.newBuilder()
                    .setGisJoin(gisJoin)
                    .setTotalIterations(lrModel.getTotalIterations())
                    .setRmseResidual(lrModel.getRmse())
                    .setR2Residual(lrModel.getR2())
                    .setIntercept(lrModel.getIntercept())
                    .addAllSlopeCoefficients(lrModel.getCoefficients())
                    .addAllObjectiveHistory(lrModel.getObjectiveHistory())
                    .build();

            return ModelResponse.newBuilder()
                    .setLinearRegressionResponse(modelResults)
                    .build();
        }
        log.error("Expected model to be of instance LRModel, got {} instead", model.getClass().getSimpleName());
        return null;
    }
}