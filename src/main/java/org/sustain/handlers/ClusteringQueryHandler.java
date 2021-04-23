package org.sustain.modeling;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sustain.BisectingKMeansRequest;
import org.sustain.BisectingKMeansResponse;
import org.sustain.Collection;
import org.sustain.GaussianMixtureRequest;
import org.sustain.GaussianMixtureResponse;
import org.sustain.KMeansClusteringRequest;
import org.sustain.KMeansClusteringResponse;
import org.sustain.LatentDirichletAllocationRequest;
import org.sustain.LatentDirichletAllocationResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ClusteringQueryHandler {
    private static final Logger log = LogManager.getFormatterLogger(ClusteringQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;
    private JavaSparkContext sparkContext;

    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void initSparkSession(ModelType modelType) {
        String resolution = "";
        switch (modelType) {
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
        log.info("resolution: " + resolution);
        String databaseUrl = "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT;
        String collection = resolution + "_stats";
        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Clustering: " + new Date())
                .config("spark.mongodb.input.uri", databaseUrl + "/" + Constants.DB.NAME + "." + collection)
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        addClusterDependencyJars(sparkContext);
    }

    public void handleQuery(ModelType modelType) {
        logRequest(modelType);
        initSparkSession(modelType);
        switch (modelType) {
            case K_MEANS_CLUSTERING:
                buildKMeansModel();
                break;
            case BISECTING_K_MEANS:
                buildBisectingKMeansModel();
                break;
            case GAUSSIAN_MIXTURE:
                buildGaussianMixtureModel();
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                buildLatentDirichletAllocationModel();
                break;
        }
        sparkContext.close();
    }

    private void buildLatentDirichletAllocationModel() {
        int k = request.getLatentDirichletAllocationRequest().getClusterCount();
        int maxIterations = request.getLatentDirichletAllocationRequest().getMaxIterations();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF();

        // LDA
        LDA lda = new LDA().setK(k).setMaxIter(maxIterations);
        LDAModel model = lda.fit(featureDF);

        double ll = model.logLikelihood(featureDF);
        double lp = model.logPerplexity(featureDF);
        log.info("LDA: Lower bound on the log likelihood of the entire corpus: " + ll);
        log.info("LDA: Upper bound on perplexity: " + lp);

        // results
        Dataset<Row> predictDF = model.transform(featureDF);
        log.info("Predictions...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing LatentDirichletAllocationResponse to stream");
        for (ClusteringResult result : results) {
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

    private void buildKMeansModel() {
        int k = request.getKMeansClusteringRequest().getClusterCount();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF();
        // KMeans Clustering
        KMeans kmeans = new KMeans().setK(k).setSeed(1L);
        KMeansModel model = kmeans.fit(featureDF);

        Vector[] vectors = model.clusterCenters();
        log.info("======================== CLUSTER CENTERS =====================================");
        for (Vector vector : vectors) {
            log.info(vector.toString());
        }

        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing KMeansClusteringResponses to stream");
        for (ClusteringResult result : results) {
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

    private void buildBisectingKMeansModel() {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF();
        int k = request.getBisectingKMeansRequest().getClusterCount();
        int maxIterations = request.getBisectingKMeansRequest().getMaxIterations();

        BisectingKMeans bisectingKMeans = new BisectingKMeans().setK(k).setMaxIter(maxIterations);
        BisectingKMeansModel model = bisectingKMeans.fit(featureDF);

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing BisectingKMeansResponses to stream");
        for (ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setBisectingKMeansResponse(
                            BisectingKMeansResponse.newBuilder()
                                    .setGisJoin(result.getGisJoin())
                                    .setPrediction(result.getPrediction())
                                    .build()
                    ).build()
            );
        }
    }

    private void buildGaussianMixtureModel() {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF();
        int k = request.getGaussianMixtureRequest().getClusterCount();
        int maxIterations = request.getGaussianMixtureRequest().getMaxIterations();

        GaussianMixture gaussianMixture = new GaussianMixture().setK(k).setMaxIter(maxIterations);
        GaussianMixtureModel model = gaussianMixture.fit(featureDF);


        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
                    i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing GaussianMixtureResponses to stream");
        for (ClusteringResult result : results) {
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

    private Dataset<Row> preprocessAndGetFeatureDF() {
        log.info("Preprocessing data");
        ReadConfig readConfig = ReadConfig.create(sparkContext);
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        Seq<String> features = convertListToSeq(featuresList);

        Dataset<Row> selectedFeatures = collection.select(Constants.GIS_JOIN, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

        // Assembling
        VectorAssembler assembler =
                new VectorAssembler().setInputCols(featuresList.toArray(new String[0])).setOutputCol("features");
        Dataset<Row> featureDF = assembler.transform(selectedFeatures);
        featureDF.show(10);

        // Scaling
        log.info("Normalizing features");
        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("normalized_features");
        MinMaxScalerModel scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        log.info("Dataframe after min-max normalization");
        featureDF.show(10);

        return featureDF;
    }


    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    /**
     * Adds required dependency jars to the Spark Context member.
     */
    private void addClusterDependencyJars(JavaSparkContext sparkContext) {
        String[] jarPaths = {
                "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
                "build/libs/spark-core_2.12-3.0.1.jar",
                "build/libs/spark-mllib_2.12-3.0.1.jar",
                "build/libs/spark-sql_2.12-3.0.1.jar",
                "build/libs/bson-4.0.5.jar",
                "build/libs/mongo-java-driver-3.12.5.jar"
        };

        for (String jar : jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: " + jar);
            sparkContext.addJar(jar);
        }
    }

    private void logRequest(ModelType modelType) {
        log.info("Logging " + modelType.toString());
        log.info("============== REQUEST ===============");
        log.info("Collections:");
        for (int i = 0; i < this.request.getCollectionsCount(); i++) {
            Collection col = this.request.getCollections(i);
            log.info("\tName: " + col.getName());
            log.info("\tLabel: " + col.getLabel());
            log.info("\tFeatures:");
            for (int j = 0; j < col.getFeaturesCount(); j++) {
                log.info("\t\t" + col.getFeatures(j));
            }
        }
        switch (modelType) {
            case K_MEANS_CLUSTERING:
                KMeansClusteringRequest kMeansClusteringRequest = request.getKMeansClusteringRequest();
                log.info("\tClusterCount: " + kMeansClusteringRequest.getClusterCount());
                log.info("\tMaxIterations: " + kMeansClusteringRequest.getMaxIterations());
                break;
            case BISECTING_K_MEANS:
                BisectingKMeansRequest bisectingKMeansRequest = request.getBisectingKMeansRequest();
                log.info("\tClusterCount: " + bisectingKMeansRequest.getClusterCount());
                log.info("\tMaxIterations: " + bisectingKMeansRequest.getMaxIterations());
                break;
            case GAUSSIAN_MIXTURE:
                GaussianMixtureRequest gaussianMixtureRequest = request.getGaussianMixtureRequest();
                log.info("\tClusterCount: " + gaussianMixtureRequest.getClusterCount());
                log.info("\tMaxIterations: " + gaussianMixtureRequest.getMaxIterations());
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                LatentDirichletAllocationRequest latentDirichletAllocationRequest =
                        request.getLatentDirichletAllocationRequest();
                log.info("\tClusterCount: " + latentDirichletAllocationRequest.getClusterCount());
                log.info("\tMaxIterations: " + latentDirichletAllocationRequest.getMaxIterations());
                break;
        }
    }

    private static class ClusteringResult {
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
