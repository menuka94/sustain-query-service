package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LinearRegressionModel {

    protected static final Logger log = LogManager.getLogger(LinearRegressionModel.class);

    public static void main(String[] args) {
        log.info("Running LinearRegressionModel main()");

        try {
            SparkSession sparkSession = SparkSession.builder()
                    .master("spark://lattice-165:8079")
                    .config("spark.mongodb.input.uri", "mongodb://lattice-46:27017")
                    .config("spark.mongodb.input.database", "sustaindb")
                    .config("spark.mongodb.input.collection", "future_heat")
                    .appName("LinearRegressionModel")
                    .getOrCreate();

            // Initialize Spark Context
            JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

            // Add dependency jar files to Spark Context - TODO fix hardcoded values
            sparkContext.addJar("build/install/lib/mongo-spark-connector_2.12-3.0.0.jar");
            sparkContext.addJar("build/install/lib/spark-core_2.12-3.0.0.jar");
            sparkContext.addJar("build/install/lib/spark-mllib_2.12-3.0.0.jar");
            sparkContext.addJar("build/install/lib/spark-sql_2.12-3.0.0.jar");

            String gisJoin = "G1201050";

            log.info("HELLO WORLD");

            // Close Spark Context
            sparkContext.close();
        } catch (Exception e) {
            log.error("Failed to create SparkSession: " + e.getMessage());
        }

    }




}
