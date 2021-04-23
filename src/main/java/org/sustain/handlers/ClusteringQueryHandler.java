package org.sustain.handlers;

import com.google.gson.annotations.SerializedName;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.handlers.clustering.BisectingKMeansClusteringHandlerImpl;
import org.sustain.handlers.clustering.GaussianMixtureClusteringHandlerImpl;
import org.sustain.handlers.clustering.KMeansClusteringHandlerImpl;
import org.sustain.handlers.clustering.LDAClusteringHandlerImpl;
import org.sustain.util.Constants;
import org.sustain.util.ProfilingUtil;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ClusteringQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {

    private static final Logger log = LogManager.getLogger(ClusteringQueryHandler.class);

    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver,
                                  SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean isValid(ModelRequest modelRequest) {
        // TODO: Implement
        return true;
    }

    @Override
    public void handleRequest() {
        this.logRequest(request);

        try {
            // Submit task to Spark Manager
            Future<Boolean> future =
                this.sparkManager.submit(this, "clustering-query");

            // Wait for task to complete
            future.get();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext) {
        switch (request.getType()) {
            case K_MEANS_CLUSTERING:
                buildKMeansModel(sparkContext);
                break;
            case BISECTING_K_MEANS:
                buildBisectingKMeansModel(sparkContext);
                break;
            case GAUSSIAN_MIXTURE:
                buildGaussianMixtureModel(sparkContext);
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                buildLatentDirichletAllocationModel(sparkContext);
                break;
        }

        return true;
    }

    private void buildLatentDirichletAllocationModel(JavaSparkContext sparkContext) {
        int k = request.getLatentDirichletAllocationRequest().getClusterCount();
        int maxIterations = request.getLatentDirichletAllocationRequest().getMaxIterations();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);

        LDAClusteringHandlerImpl handler = new LDAClusteringHandlerImpl();
        Dataset<Row> predictDF = handler.buildModel(k, maxIterations, featureDF);

        handler.writeToStream(predictDF, responseObserver);

        handler.buildModelWithPCA(k, maxIterations, 2, featureDF);
    }

    private void buildKMeansModel(JavaSparkContext sparkContext) {
        int k = request.getKMeansClusteringRequest().getClusterCount();
        int maxIterations = request.getKMeansClusteringRequest().getMaxIterations();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);

        KMeansClusteringHandlerImpl handler = new KMeansClusteringHandlerImpl();

        Dataset<Row> predictDF = handler.buildModel(k, maxIterations, featureDF);

        handler.writeToStream(predictDF, responseObserver);

        handler.buildModelWithPCA(k, maxIterations, 2, featureDF);
    }

    private void buildBisectingKMeansModel(JavaSparkContext sparkContext) {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        int k = request.getBisectingKMeansRequest().getClusterCount();
        int maxIterations = request.getBisectingKMeansRequest().getMaxIterations();

        BisectingKMeansClusteringHandlerImpl handler = new BisectingKMeansClusteringHandlerImpl();

        Dataset<Row> predictDF = handler.buildModel(k, maxIterations, featureDF);

        handler.writeToStream(predictDF, responseObserver);

        handler.buildModelWithPCA(k, maxIterations, 2, featureDF);
    }

    private void buildGaussianMixtureModel(JavaSparkContext sparkContext) {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        int k = request.getGaussianMixtureRequest().getClusterCount();
        int maxIterations = request.getGaussianMixtureRequest().getMaxIterations();

        GaussianMixtureClusteringHandlerImpl handler = new GaussianMixtureClusteringHandlerImpl();
        Dataset<Row> predictDF = handler.buildModel(k, maxIterations, featureDF);

        handler.writeToStream(predictDF, responseObserver);

        handler.buildModelWithPCA(k, maxIterations, 2, featureDF);
    }

    private Dataset<Row> preprocessAndGetFeatureDF(JavaSparkContext sparkContext) {
        // Identify resolution of evaluation
        String resolution = "";
        switch (request.getType()) {
            case K_MEANS_CLUSTERING:
                resolution = request.getKMeansClusteringRequest().getResolution().toString().toLowerCase();
                break;
            case BISECTING_K_MEANS:
                resolution = request.getBisectingKMeansRequest().getResolution().toString().toLowerCase();
                break;
            case GAUSSIAN_MIXTURE:
                resolution = request.getGaussianMixtureRequest().getResolution().toString().toLowerCase();
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                resolution = request.getLatentDirichletAllocationRequest().getResolution().toString().toLowerCase();
                break;
            case UNRECOGNIZED:
                log.warn("Invalid request type");
                break;
        }

        // Initialize mongodb read configuration
        Map<String, String> readOverrides = new HashMap();
        readOverrides.put("spark.mongodb.input.collection", resolution + "_stats");
        readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
        readOverrides.put("spark.mongodb.input.uri",
            "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig =
            ReadConfig.create(sparkContext.getConf(), readOverrides);

        // Load mongodb rdd and convert to dataset
        log.info("Preprocessing data");
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        Seq<String> features = convertListToSeq(featuresList);

        Dataset<Row> selectedFeatures = collection.select(Constants.GIS_JOIN, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

        // Assembling
        long assemblyTime1 = System.currentTimeMillis();

        VectorAssembler assembler =
            new VectorAssembler().setInputCols(featuresList.toArray(new String[0])).setOutputCol("features");
        Dataset<Row> featureDF = assembler.transform(selectedFeatures);
        featureDF.show(10);

        long assemblyTime2 = System.currentTimeMillis();
        ProfilingUtil.calculateTimeDiff(assemblyTime1, assemblyTime2, "AssemblyTime");

        // Scaling
        log.info("Normalizing features");
        long scalingTime1 = System.currentTimeMillis();
        MinMaxScaler scaler = new MinMaxScaler()
            .setInputCol("features")
            .setOutputCol("normalized_features");
        MinMaxScalerModel scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        long scalingTime2 = System.currentTimeMillis();
        ProfilingUtil.calculateTimeDiff(scalingTime1, scalingTime2, "ScalingTime");
        ProfilingUtil.calculateTimeDiff(assemblyTime1, scalingTime2, "AssemblyAndScalingTime");

        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        log.info("Dataframe after min-max normalization");
        featureDF.show(10);

        return featureDF;
    }


    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static class ClusteringResult {
        @SerializedName("GISJOIN")
        String gisJoin;
        int prediction;

        public String getGisJoin() {
            return gisJoin;
        }

        public int getPrediction() {
            return prediction;
        }

        @Override
        public String toString() {
            return "ClusteringResult{" +
                "gisJoin='" + gisJoin + '\'' +
                ", prediction=" + prediction +
                '}';
        }
    }
}
