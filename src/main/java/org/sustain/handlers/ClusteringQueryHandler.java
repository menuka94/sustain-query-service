package org.sustain.handlers;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sustain.Collection;
import org.sustain.KMeansClusteringRequest;
import org.sustain.KMeansClusteringResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Type;
import java.util.ArrayList;
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

    public void handleQuery() {
        logRequest();
        initSparkSession();
        buildModel();
        sparkContext.close();
    }

    public void initSparkSession() {
        String resolution = request.getKMeansClusteringRequest().getResolution().toString().toLowerCase();
        log.info("resolution: " + resolution);
        String databaseUrl = "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT;
        String collection = resolution + "_stats";
        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Clustering")
                .config("spark.mongodb.input.uri", databaseUrl + "/" + Constants.DB.NAME + "." + collection)
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        addClusterDependencyJars(sparkContext);
    }

    private void buildModel() {
        ReadConfig readConfig = ReadConfig.create(sparkContext);
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();

        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        int k = request.getKMeansClusteringRequest().getClusterCount();
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
        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("normalized_features");
        MinMaxScalerModel scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        featureDF.show(10);

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

        for (ClusteringResult result : results) {
            //write results to gRPC response
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

    private void logRequest() {
        KMeansClusteringRequest kMeansClusteringRequest = request.getKMeansClusteringRequest();

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

        log.info("KMeansClusteringRequest:");
        KMeansClusteringRequest req = this.request.getKMeansClusteringRequest();
        log.info("\tClusterCount: " + kMeansClusteringRequest.getClusterCount());
        log.info("\tMaxIterations: " + kMeansClusteringRequest.getMaxIterations());
        log.info("=======================================");
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
