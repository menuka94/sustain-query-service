/* ---------------------------------------------------------------------------------------------------------------------
 * LRModel.java -
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
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sustain.util.Constants;
import org.sustain.util.Task;
import scala.collection.JavaConverters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.*;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class LRModel {

    protected static final Logger log = LogManager.getLogger(LRModel.class);

    private Dataset<Row>     mongoCollection;
    private List<String>     features;
    private String           gisJoin, label, loss, solver;
    private Integer          aggregationDepth, maxIterations, totalIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance, rmse, r2, intercept;
    private List<Double>     coefficients, objectiveHistory;
    private Boolean          fitIntercept, setStandardization;


    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private LRModel() {}


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
        cols.addAll(this.features);
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

    /**
     * Trains a Linear Regression model for a single GISJoin.
     */
    public void train() {

        // Begin a profiling task for how long it takes to train this gisJoin
        log.info(">>> Building model for GISJoin {}", this.gisJoin);
        Task trainTask = new Task(String.format("LRModel train(%s)", this.gisJoin), 0);

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = this.mongoCollection.select("_id", desiredColumns());

        // Filter collection by our GISJoin
        Dataset<Row> gisDataset = selected.filter(selected.col("gis_join").$eq$eq$eq(this.gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"

        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(this.features.toArray(new String[0]))
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);

        // Create a SparkML Linear Regression object using user-specified parameters
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

        trainTask.finish();
        log.info(">>> Finished building model for GISJoin: {}, Task: {}", this.gisJoin, trainTask);
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Linear Regression Model")
                .config("spark.mongodb.input.uri", String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT))
                .config("spark.mongodb.input.database", Constants.DB.NAME)
                .config("spark.mongodb.input.collection", "maca_v2")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        LRModel lrModel = new LRModelBuilder()
                .forMongoCollection(MongoSpark.load(sparkContext, readConfig).toDF())
                .forGISJoin("G0100290") // Cleburne County, Alabama
                .forFeatures(Collections.singletonList("singleton"))
                .forLabel("max_max_air_temperature")
                .withMaxIterations(100)
                .withEpsilon(1.35)
                .withTolerance(1E-7)
                .build();


        lrModel.train();
        log.info("Executed LRModel.main() successfully");
        sparkContext.close();
    }

    /**
     * Builder class for the LRModel object.
     */
    public static class LRModelBuilder implements ModelBuilder<LRModel> {

        private Dataset<Row>     mongoCollection;
        private List<String>     features;
        private String           gisJoin, label;

        // Model parameters and their defaults
        private String           loss="squaredError", solver="auto";
        private Integer          aggregationDepth=2, maxIterations=10;
        private Double           elasticNetParam=0.0, epsilon=1.35, regularizationParam=0.5, convergenceTolerance=1E-6;
        private Boolean          fitIntercept=true, setStandardization=true;

        public LRModelBuilder forMongoCollection(Dataset<Row> mongoCollection) {
            this.mongoCollection = mongoCollection;
            return this;
        }

        public LRModelBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public LRModelBuilder forLabel(String label) {
            this.label = label;
            return this;
        }

        public LRModelBuilder forFeatures(List<String> features) {
            this.features = features;
            return this;
        }

        public LRModelBuilder withLoss(String loss) {
            if (!loss.isBlank()) {
                this.loss = loss;
            }
            return this;
        }

        public LRModelBuilder withSolver(String solver) {
            if (!solver.isBlank()) {
                this.solver = solver;
            }
            return this;
        }

        public LRModelBuilder withAggregationDepth(Integer aggregationDepth) {
            if (aggregationDepth != null && aggregationDepth >= 2 && aggregationDepth <= 10) {
                this.aggregationDepth = aggregationDepth;
            }
            return this;
        }

        public LRModelBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations < 100) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public LRModelBuilder withElasticNetParam(Double elasticNetParam) {
            if ((elasticNetParam != null) && elasticNetParam >= 0.0 && elasticNetParam <= 1.0 ) {
                this.elasticNetParam = elasticNetParam;
            }
            return this;
        }

        public LRModelBuilder withEpsilon(Double epsilon) {
            if (epsilon != null && epsilon > 1.0 && epsilon <= 10.0) {
                this.epsilon = epsilon;
            }
            return this;
        }

        public LRModelBuilder withRegularizationParam(Double regularizationParam) {
            if (regularizationParam != null && regularizationParam >= 0.0 && regularizationParam <= 10.0 ) {
                this.regularizationParam = regularizationParam;
            }
            return this;
        }

        public LRModelBuilder withTolerance(Double convergenceTolerance) {
            if (convergenceTolerance != null && convergenceTolerance >= 0.0 && convergenceTolerance <= 10.0 )
            this.convergenceTolerance = convergenceTolerance;
            return this;
        }

        public LRModelBuilder withFitIntercept(Boolean fitIntercept) {
            if (fitIntercept != null) {
                this.fitIntercept = fitIntercept;
            }
            return this;
        }

        public LRModelBuilder withStandardization(Boolean setStandardization) {
            if (setStandardization != null) {
                this.setStandardization = setStandardization;
            }
            return this;
        }

        @Override
        public LRModel build() {
            LRModel model = new LRModel();
            model.mongoCollection = this.mongoCollection;
            model.gisJoin = this.gisJoin;
            model.features = this.features;
            model.label = this.label;
            model.loss = this.loss;
            model.solver = this.solver;
            model.aggregationDepth = this.aggregationDepth;
            model.maxIterations = this.maxIterations;
            model.elasticNetParam = this.elasticNetParam;
            model.epsilon = this.epsilon;
            model.regularizationParam = this.regularizationParam;
            model.convergenceTolerance = this.convergenceTolerance;
            model.fitIntercept = this.fitIntercept;
            model.setStandardization = this.setStandardization;
            return model;
        }
    }

}
