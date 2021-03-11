/* ---------------------------------------------------------------------------------------------------------------------
 * LinearRegressionModel.java -
 *      Defines a generalized linear regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Caleb Carlson
 * ------------------------------------------------------------------------------------------------------------------ */

package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.*;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class SustainLinearRegression {

    protected static final Logger log = LogManager.getLogger(SustainLinearRegression.class);

    private JavaSparkContext sparkContext;
    private String[]         features;
    private String           gisJoin, label, loss, solver;
    private Integer          aggregationDepth, maxIterations, totalIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance, rmse, r2, intercept;
    private List<Double>     coefficients, objectiveHistory;
    private Boolean          fitIntercept, setStandardization;


    /**
     * The default, overloaded constructor for the SustainLinearRegression class. Defines Mongo and Spark connection
     * parameters, as well as what data we are using for the model.
     * @param master The Spark master in the form spark://<host>:<port>
     * @param mongoUri The MongoDB endpoint, most likely a Mongo Router, in the form mongodb://<host>:<port>
     * @param database The name of the MongoDB database we are accessing
     * @param collection The name of the MongoDB collection we are using for the model
     */
    public SustainLinearRegression(String master, String mongoUri, String database, String collection, String gisJoin) {
        initSparkSession(master, mongoUri, database, collection);
        setGisJoin(gisJoin);
        setModelParameterDefaults();
        addClusterDependencyJars();
    }

    /**
     * Set default model parameters before user explicitly defines them.
     * Uses Spark's default values which can be found in the documentation here:
     * https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/regression/LinearRegression.html
     */
    private void setModelParameterDefaults() {
        this.setLoss("squaredError");
        this.setSolver("auto");
        this.setAggregationDepth(2);
        this.setMaxIterations(10);
        this.setElasticNetParam(0.0);
        this.setEpsilon(1.35);
        this.setRegularizationParam(0.5);
        this.setConvergenceTolerance(1E-6); // 1E-6 = 0.000001
        this.setFitIntercept(true);
        this.setSetStandardization(true);
    }

    /**
     * Configures and builds a SparkSession and JavaSparkContext, then adds required dependency JARs to the cluster.
     * @param master URI of the Spark master. Format: spark://<hostname>:<port>
     * @param mongoUri URI of the Mongo database router. Format: mongodb://<hostname>:<port>
     * @param database Name of the Mongo database to use.
     * @param collection Name of the Mongo collection to import from above database.
     */
    private void initSparkSession(String master, String mongoUri, String database, String collection) {
        log.info("Initializing SparkSession using:\n\tmaster={}\n\tspark.mongodb.input.uri={}" +
                "\n\tspark.mongodb.input.database={}\n\tspark.mongodb.input.collection={}",
                master, mongoUri, database, collection);

        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("SUSTAIN Linear Regression Model")
                .config("spark.mongodb.input.uri", mongoUri) // mongodb://lattice-46:27017
                .config("spark.mongodb.input.database", database) // sustaindb
                .config("spark.mongodb.input.collection", collection) // future_heat
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        addClusterDependencyJars();
    }

    /**
     * Adds required dependency jars to the Spark Context member.
     */
    private void addClusterDependencyJars() {
        String[] jarPaths = {
            "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
            "build/libs/spark-core_2.12-3.0.1.jar",
            "build/libs/spark-mllib_2.12-3.0.1.jar",
            "build/libs/spark-sql_2.12-3.0.1.jar",
            "build/libs/bson-4.0.5.jar",
            "build/libs/mongo-java-driver-3.12.5.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }
    }

    public void setFeatures(String[] features) {
        this.features = features;
    }

    public void setGisJoin(String gisJoin) {
        this.gisJoin = gisJoin;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLoss(String loss) {
        this.loss = loss;
    }

    public void setSolver(String solver) {
        this.solver = solver;
    }

    public void setAggregationDepth(Integer aggregationDepth) {
        this.aggregationDepth = aggregationDepth;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    public void setElasticNetParam(Double elasticNetParam) {
        this.elasticNetParam = elasticNetParam;
    }

    public void setEpsilon(Double epsilon) {
        this.epsilon = epsilon;
    }

    public void setRegularizationParam(Double regularizationParam) {
        this.regularizationParam = regularizationParam;
    }

    public void setConvergenceTolerance(Double convergenceTolerance) {
        this.convergenceTolerance = convergenceTolerance;
    }

    public void setFitIntercept(Boolean fitIntercept) {
        this.fitIntercept = fitIntercept;
    }

    public void setSetStandardization(Boolean setStandardization) {
        this.setStandardization = setStandardization;
    }

    public String getGisJoin() {
        return gisJoin;
    }

    public Double getRmse() {
        return rmse;
    }

    public Double getR2() {
        return r2;
    }

    public Double getIntercept() {
        return intercept;
    }

    public List<Double> getCoefficients() {
        return coefficients;
    }

    public List<Double> getObjectiveHistory() {
        return objectiveHistory;
    }

    public Integer getTotalIterations() {
        return totalIterations;
    }

    /**
     * Compiles a List<String> of column names we desire from the loaded collection, using the features String array.
     * @return A Scala Seq<String> of desired column names.
     */
    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add("gis_join");
        Collections.addAll(cols, this.features);
        cols.add(this.label);
        return convertListToSeq(cols);
    }

    /**
     * Converts a Java List<String> of inputs to a Scala Seq<String>
     * @param inputList The Java List<String> we wish to transform
     * @return A Scala Seq<String> representing the original input list
     */
    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public void buildAndRunModel() {
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        // Lazy-load the collection in as a DF
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = collection.select("_id", desiredColumns());

        log.info(">>> Building model for GISJoin {}", gisJoin);

        // Filter by the current GISJoin so we only get records corresponding to the current GISJoin
        //FilterFunction<Row> ff = row -> row.getAs("gis_join") == gisJoin;

        Dataset<Row> gisDataset = selected.filter(selected.col("gis_join").$eq$eq$eq(gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"

        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(this.features)
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);
        mergedDataset.show(5);

        // Create Linear Regression object using user-specified parameters
        LinearRegression linearRegression = new LinearRegression()
                .setLoss(this.loss)
                .setSolver(this.solver)
                .setAggregationDepth(this.aggregationDepth)
                .setMaxIter(this.maxIterations)
                .setEpsilon(this.epsilon)
                .setElasticNetParam(this.elasticNetParam)
                .setRegParam(this.regularizationParam)
                .setTol(this.convergenceTolerance)
                .setFitIntercept(this.fitIntercept)
                .setStandardization(this.setStandardization);

        // Fit the dataset with the "features" and "label" columns
        LinearRegressionModel lrModel = linearRegression.fit(mergedDataset);

        // Save training summary
        LinearRegressionTrainingSummary summary = lrModel.summary();

        this.coefficients = new ArrayList<>();
        double[] primitiveCoefficients = lrModel.coefficients().toArray();
        for (double d: primitiveCoefficients) {
            this.coefficients.add(d);
        }

        this.objectiveHistory = new ArrayList<>();
        double[] primitiveObjHistory = summary.objectiveHistory();
        for (double d: primitiveObjHistory) {
            this.objectiveHistory.add(d);
        }

        this.intercept = lrModel.intercept();
        this.totalIterations = summary.totalIterations();
        this.rmse = summary.rootMeanSquaredError();
        this.r2 = summary.r2();

        logModelResults();

        // Don't forget to close Spark Context!
        sparkContext.close();
    }

    /**
     * Logs the Linear Model results for a single GISJoin.
     */
    private void logModelResults() {
        log.info("Results for GISJoin {}\n" +
                "Model Slope Coefficients: {}\n" +
                "Model Intercept: {}\n" +
                "Total Iterations: {}\n" +
                "Objective History: {}\n" +
                "RMSE Residual: {}\n" +
                "R2 Residual: {}\n",
                this.gisJoin, this.coefficients, this.intercept, this.totalIterations, this.objectiveHistory, this.rmse,
                this.r2);
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {
        /*
        String[] features = {"timestamp"};
        String label = "max_max_air_temperature";

        SustainLinearRegression lrModel = new SustainLinearRegression("spark://lattice-165:8079", "testApplication",
                "mongodb://lattice-46:27017", "sustaindb", "macav2");

        lrModel.setFeatures(features);
        lrModel.setLabel(label);
        lrModel.setGisJoin("G0100290"); // Cleburne County, Alabama

        lrModel.setConvergenceTolerance(1E-14);
        lrModel.setMaxIterations(100);
        lrModel.setEpsilon(1.35);

        lrModel.buildAndRunModel();

         */

        log.info("Executed SustainLinearRegression.main() successfully");
    }

}
