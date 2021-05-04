package org.sustain.handlers.clustering;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.KMeansClusteringResponse;
import org.sustain.ModelResponse;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.util.Constants;
import org.sustain.util.ProfilingUtil;

import java.lang.reflect.Type;
import java.util.List;

public class KMeansClusteringHandlerImpl extends AbstractClusteringHandler {
    private static final Logger log = LogManager.getLogger(KMeansClusteringHandlerImpl.class);

    @Override
    public Dataset<Row> buildModel(int clusterCount, int maxIterations, Dataset<Row> featureDF) {
        long buildTime1 = System.currentTimeMillis();
        KMeans kmeans = new KMeans().setK(clusterCount).setMaxIter(maxIterations);

        KMeansModel model = kmeans.fit(featureDF);
        long buildTime2 = System.currentTimeMillis();

        ProfilingUtil.calculateTimeDiff(buildTime1, buildTime2, "KMeansModelBuildTime");

        Vector[] vectors = model.clusterCenters();
        log.info("======================== CLUSTER CENTERS =====================================");
        for (Vector vector : vectors) {
            log.info(vector.toString());
        }

        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions...");
        predictDF.show(10);

        // evaluate clustering results
        Dataset<Row> evaluateDF = model.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF, "KMeans", String.format("without PCA, k=%d", clusterCount));

        return predictDF;
    }

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

        KMeans kMeans1 = new KMeans().setK(clusterCount).setMaxIter(maxIterations);
        KMeansModel model1 = kMeans1.fit(featureDF1);
        Dataset<Row> predictDF1 = model1.transform(featureDF1).select(Constants.GIS_JOIN, "prediction");
        predictDF1.show();

        // evaluate clustering (with PCA) results
        Dataset<Row> evaluateDF1 = model1.transform(featureDF1).select(Constants.GIS_JOIN, "features", "prediction");
        ProfilingUtil.evaluateClusteringModel(evaluateDF1, "KMeans",
                String.format("with PCA, k=%d, #PC=%d", clusterCount, principalComponentCount));
    }

    public void writeToStream(Dataset<Row> predictDF, StreamObserver<ModelResponse> responseObserver) {
        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringQueryHandler.ClusteringResult>>() {
        }.getType();
        List<ClusteringQueryHandler.ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing KMeansClusteringResponses to stream");
        for (ClusteringQueryHandler.ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                .setKMeansClusteringResponse(
                    KMeansClusteringResponse.newBuilder()
                        .setGisJoin(result.getGisJoin())
                        .setPrediction(result.getPrediction())
                        .build()
                ).build()
            );
        }
    }
}
