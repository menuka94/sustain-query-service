package org.sustain.handlers.clustering;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.LatentDirichletAllocationResponse;
import org.sustain.ModelResponse;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.util.Constants;
import org.sustain.util.ProfilingUtil;

import java.lang.reflect.Type;
import java.util.List;

public class LDAClusteringHandlerImpl extends AbstractClusteringHandler {
    private static final Logger log = LogManager.getLogger(LDAClusteringHandlerImpl.class);

    @Override
    public Dataset<Row> buildModel(int k, int maxIterations, Dataset<Row> featureDF) {
        long buildTime1 = System.currentTimeMillis();
        LDA lda = new LDA().setK(k).setMaxIter(maxIterations);
        LDAModel model = lda.fit(featureDF);
        long buildTime2 = System.currentTimeMillis();
        ProfilingUtil.calculateTimeDiff(buildTime1, buildTime2, "LDAModelBuildTime");

        double ll = model.logLikelihood(featureDF);
        double lp = model.logPerplexity(featureDF);
        log.info("LDA: Lower bound on the log likelihood of the entire corpus: " + ll);
        log.info("LDA: Upper bound on perplexity: " + lp);

        // results
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions...");
        predictDF.show(10);

        // evaluate clustering results
        Dataset<Row> evaluateDF = model.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF, "LDA", "without PCA");

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
            .select("GISJOIN", "features");
        featureDF1.show();

        LDA lda = new LDA().setK(clusterCount).setMaxIter(maxIterations);
        LDAModel model = lda.fit(featureDF1);

        double ll = model.logLikelihood(featureDF1);
        double lp = model.logPerplexity(featureDF1);
        log.info("LDA: Lower bound on the log likelihood of the entire corpus: " + ll);
        log.info("LDA: Upper bound on perplexity: " + lp);

        // results
        Dataset<Row> predictDF = model.transform(featureDF1).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions...");
        predictDF.show(10);

        // evaluate clustering results
        Dataset<Row> evaluateDF = model.transform(featureDF1).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF, "LDA", "with PCA");
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

        log.info("Writing LatentDirichletAllocationResponse to stream");
        for (ClusteringQueryHandler.ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                .setLatentDirichletAllocationResponse(
                    LatentDirichletAllocationResponse.newBuilder()
                        .setGisJoin(result.getGisJoin())
                        .setPrediction(result.getPrediction())
                        .build()
                ).build()
            );
        }
    }
}
