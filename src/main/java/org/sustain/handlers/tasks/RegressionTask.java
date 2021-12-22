package org.sustain.handlers.tasks;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.sustain.Collection;
import org.sustain.ModelResponse;
import org.sustain.modeling.SustainRegressionModel;
import org.sustain.util.Constants;

import java.util.*;

public abstract class RegressionTask implements SparkTask<List<ModelResponse>> {

    private static final Logger log = LogManager.getLogger(RegressionTask.class);

    // Original gRPC Collection object containing name, label, and features
    public Collection requestCollection;

    // List of GISJOINs in the batch, each of which we need to create a model for
    public List<String> gisJoins;

    /**
     * Creates a ReadConfig override for MongoSpark Connector, using the collection name passed in.
     * @param sparkContext JavaSparkContext used with MongoSpark Connector.
     * @param collectionName MongoDB Collection name.
     * @return A new ReadConfig for overriding the default one.
     */
    public static ReadConfig createReadConfig(JavaSparkContext sparkContext, String collectionName) {
        Map<String, String> readOverrides = new HashMap<>();
        String mongoUri = String.format("mongodb://%s:%s/", Constants.DB.HOST, Constants.DB.PORT);
        readOverrides.put("uri", mongoUri);
        readOverrides.put("database", Constants.DB.NAME);
        readOverrides.put("collection", collectionName);
        readOverrides.put("readConcern.level", "available");
        return ReadConfig.create(sparkContext.getConf(), readOverrides);
    }

    /**
     * Creates a VectorAssembler to assemble all feature columns of the input Dataset into a single column vector
     * named "features". Example: Given the following row:
     * | Feature_Column_A | Feature_Column_B | Feature_Column_C | label |
     * | 12.35            | 15.68            | 19.23            | 9.2   |
     *
     * the following transformation is applied:
     * | features              | label |
     * | [12.35, 15.68, 19.23] | 9.2   |
     * @param inputDataset Dataset<Row> containing all the unmerged named feature columns
     * @return Dataset<Row> containing only two columns: "features", and "label"
     */
    public static Dataset<Row> createFeaturesVectorColumn(Dataset<Row> inputDataset, List<String> features) {
        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(features.toArray(new String[0]))
                .setOutputCol("features");

        // Transform to have the new "features" vector column, and select only the "features" and "label" columns
        return vectorAssembler.transform(inputDataset).select("features", "label");
    }

    /**
     * Splits the full Dataset into train and test Datasets, based on the trainSetPercent.
     * @param inputDataset The full Dataset.
     * @param trainSetPercent Integer percent value of the training set, between [1, 100]
     * @return A size-2 array of Datasets, the first being for training, the second being for testing.
     */
    public static Dataset<Row>[] getTrainAndTestSplits(Dataset<Row> inputDataset, int trainSetPercent) {
        double trainPercentNormalized = trainSetPercent / 100.0;
        double[] splitValues = new double[] {trainPercentNormalized, 1.0 - trainPercentNormalized};
        return inputDataset.randomSplit(splitValues);
    }

    /**
     * Loads and processes a Mongo Collection into a Spark Dataset<Row>, filtered by GISJOIN, with the requested
     * features assembled into a "features" vector column, and "label" column.
     * @param sparkContext JavaSparkContext used for the MongoSpark Connector
     * @param collectionName Name of the collection in MongoDB
     * @param features List<String> of features to use
     * @param label String name of feature to use as label
     * @param gisJoin String spatial GISJOIN identifier to filter by (in MongoDB pipeline $match)
     * @return Dataset<Row> containing a "GISJOIN" column, vector "features" column, and "label" column.
     */
    public static Dataset<Row> loadAndProcessDataset(JavaSparkContext sparkContext, String collectionName,
                                                     List<String> features, String label, String gisJoin) {
        log.info("Loading and processing collection {}", collectionName);

        // Load Dataset (lazily, really just connect to Mongo Router)
        JavaMongoRDD<Document> mongoCollectionRdd = MongoSpark.load(
                sparkContext,
                createReadConfig(sparkContext, collectionName)
        );

        // Specify MongoDB pipeline for loading data
        String matchStage = String.format("{ $match: { \"GISJOIN\": \"%s\" } }", gisJoin); // only get records for matching gisJoin
        String projectStage = String.format("{ $project: { " +
                "\"_id\": 0, " + // exclude _id field
                "\"%s\":  1, " +   // label
                "\"%s\":  1 " +  // feature
                "} }", label, features.get(0)
        );
        log.info("MongoDB Aggregation Pipeline: [\n\t{},\n\t{}\n]\n", matchStage, projectStage);
        JavaMongoRDD<Document> aggregatedRdd = mongoCollectionRdd.withPipeline(
                Arrays.asList(
                        Document.parse(matchStage),
                        Document.parse(projectStage)
                )
        );

        // Pull collection from MongoDB into Spark and print inferred schema
        Dataset<Row> mongoCollectionDs = aggregatedRdd.toDF();
        mongoCollectionDs.printSchema();

        // Rename label column to "label"
        Dataset<Row> labeledDs = mongoCollectionDs.withColumnRenamed(
                label, "label"
        );

        // Create "features" vector column, selecting only "features"/"label" columns, then return
        return createFeaturesVectorColumn(labeledDs, features);
    }

    /**
     * Executes a regression model training task.
     * @param sparkContext JavaSparkContext for our Spark application.
     * @return A List of ModelResponses, one per GISJOIN model.
     * @throws Exception If shit hits the fan
     */
    @Override
    public List<ModelResponse> execute(JavaSparkContext sparkContext) throws Exception {
        List<ModelResponse> modelResponses = new ArrayList<>();
        for (String gisJoin: this.gisJoins) {
            log.info("Building regression model for GISJOIN {}...", gisJoin);

            // Get training/testing input splits
            Dataset<Row>[] splits = getTrainAndTestSplits(
                    loadAndProcessDataset(
                            sparkContext,
                            this.requestCollection.getName(),
                            this.requestCollection.getFeaturesList(),
                            this.requestCollection.getLabel(),
                            gisJoin
                    ),
                    80
            );
            Dataset<Row> trainSet = splits[0];
            Dataset<Row> testSet  = splits[1];

            SustainRegressionModel model = buildRegressionModel();
            model.train(trainSet); // Trains a Spark RegressionModel
            model.test(testSet); // Evaluates the trained Spark RegressionModel
            modelResponses.add(buildModelResponse(gisJoin, model));
        }
        return modelResponses;
    }

    /**
     * Builds a RegressionModel, as implemented by the concrete regression subclasses.
     * @return A RegressionModel concrete instance.
     */
    public abstract SustainRegressionModel buildRegressionModel();

    /**
     * Builds a ModelResponse from the trained/tested RegressionModel, as implemented by the concrete
     * regression subclasses.
     * @param gisJoin The GISJOIN that the ModelResponse was built for
     * @param model The trained/tested RegressionModel instance
     * @return A ModelResponse to be sent back via gRPC
     */
    public abstract ModelResponse buildModelResponse(String gisJoin, SustainRegressionModel model);
}
