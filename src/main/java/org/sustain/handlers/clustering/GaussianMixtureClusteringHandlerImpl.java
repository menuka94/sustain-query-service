package org.sustain.handlers.clustering;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.GaussianMixtureResponse;
import org.sustain.ModelResponse;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.util.Constants;
import org.sustain.util.ProfilingUtil;

import java.lang.reflect.Type;
import java.util.List;

public class GaussianMixtureClusteringHandlerImpl extends AbstractClusteringHandler {
    private static final Logger log = LogManager.getLogger(GaussianMixtureClusteringHandlerImpl.class);

    @Override
    public Dataset<Row> buildModel(int clusterCount, int maxIterations, Dataset<Row> featureDF) {
        long buildTime1 = System.currentTimeMillis();
        GaussianMixture gaussianMixture = new GaussianMixture().setK(clusterCount).setMaxIter(maxIterations);

        GaussianMixtureModel model = gaussianMixture.fit(featureDF);
        long buildTime2 = System.currentTimeMillis();

        ProfilingUtil.calculateTimeDiff(buildTime1, buildTime2, "GaussianMixtureModelBuildTime");

        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
                i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        // evaluate clustering results
        Dataset<Row> evaluateDF = model.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF, "GaussianMixture", String.format("without PCA, k=%d", clusterCount));

        return predictDF;
    }

    @Override
    public void buildModelWithPCA(int clusterCount, int maxIterations, int principalComponentCount,
                                  Dataset<Row> featureDF) {
        PCAModel pca = new PCA()
            .setInputCol("features")
            .setOutputCol("pcaFeatures")
            .setK(principalComponentCount)
            .fit(featureDF);

        // Create new dataframe containing principal components
        Dataset<Row> featureDF1 = pca.transform(featureDF)
            .drop("features")
            .withColumnRenamed("pcaFeatures", "features")
            .select(Constants.GIS_JOIN, "features");
        featureDF1.show();

        GaussianMixture gaussianMixture = new GaussianMixture().setK(clusterCount).setMaxIter(maxIterations);
        GaussianMixtureModel model = gaussianMixture.fit(featureDF1);

        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
                i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF1).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        // evaluate clustering results
        Dataset<Row> evaluateDF = model.transform(featureDF1).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF, "GaussianMixture",
                String.format("with PCA, k=%d, #PC=%d", clusterCount, principalComponentCount));
    }

    @Override
    public void writeToStream(Dataset<Row> predictDF, StreamObserver<ModelResponse> responseObserver) {
        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringQueryHandler.ClusteringResult>>() {
        }.getType();
        List<ClusteringQueryHandler.ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing GaussianMixtureResponses to stream");
        for (ClusteringQueryHandler.ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                .setGaussianMixtureResponse(
                    GaussianMixtureResponse.newBuilder()
                        .setGisJoin(result.getGisJoin())
                        .setPrediction(result.getPrediction())
                        .build()
                ).build()
            );
        }
    }
}
