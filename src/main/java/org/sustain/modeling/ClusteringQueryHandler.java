package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.sustain.KMeansClusteringRequest;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ClusteringQueryHandler {
    private static final Logger log = LogManager.getFormatterLogger(ClusteringQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;


    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {
        logRequest();

        SparkSession sparkSession = SparkSession.builder()
                .master("spark://menuka-HP:7077")
                .appName("SUSTAIN Clustering Test")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb.hospitals_geo")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        addClusterDependencyJars(sparkContext);

        ReadConfig readConfig = ReadConfig.create(sparkContext);

        JavaMongoRDD<Document> rdd =
                MongoSpark.load(sparkContext, readConfig);

        // perform count evaluation
        long count = rdd.count();

        log.info("'t\t\t Count: " + count);

        // close spark context
        sparkContext.close();
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

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: " + jar);
            sparkContext.addJar(jar);
        }
    }

    private String getAbsolutePathToFile(String fileName) {
        try {
            URL res = getClass().getClassLoader().getResource(fileName);
            assert res != null;
            File file = Paths.get(res.toURI()).toFile();
            return file.getAbsolutePath();
        } catch (URISyntaxException e) {
            log.error("File " + fileName + " not found");
            e.printStackTrace();
            return null;
        }
    }

    private void logRequest() {
        KMeansClusteringRequest kMeansClusteringRequest = request.getKMeansClusteringRequest();
        int k = kMeansClusteringRequest.getClusterCount();
        int maxIterations = kMeansClusteringRequest.getMaxIterations();
        ArrayList<String> features = new ArrayList<>(kMeansClusteringRequest.getFeaturesList());
        log.info("\tk: " + k);
        log.info("\tmaxIterations: " + maxIterations);
        log.info("\tfeatures:");
        for (String feature : features) {
            log.info("\t\t" + feature);
        }
    }
}
