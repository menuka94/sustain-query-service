package org.sustain.handlers.tasks;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.Collection;
import org.sustain.ModelResponse;
import org.sustain.modeling.RegressionModel;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        Map<String, String> readOverrides = new HashMap<String, String>();
        String mongoUri = String.format("mongodb://%s:%s", Constants.DB.HOST, Constants.DB.PORT);
        readOverrides.put("uri", mongoUri);
        readOverrides.put("database", Constants.DB.NAME);
        readOverrides.put("collection", collectionName);
        return ReadConfig.create(sparkContext.getConf(), readOverrides);
    }

    /**
     * Converts a Java List<String> of inputs to a Scala Seq<String>
     * @param features The Java List<String> of column names for features we wish to use from the dataset
     * @param label The String name of the column we wish to use as the label for prediction
     * @return A Scala Seq<String> with the feature names, and label name
     */
    private static Seq<String> desiredColumnsAsSeq(List<String> features, String label) {
        List<String> columns = new ArrayList<>(features);
        columns.add(label);
        return JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();
    }

    /**
     * Creates a VectorAssembler to assemble all feature columns of inputDataset into a single column vector
     * named "features". For example:
     *
     *
     * @param inputDataset Dataset<Row> containing all the unmerged named feature columns
     * @return Dataset<Row> containing only two columns: "features", and "label"
     */
    public static Dataset<Row> createFeaturesColumn(Dataset<Row> inputDataset, List<String> features) {
        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(features.toArray(new String[0]))
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        return vectorAssembler.transform(inputDataset).select("features", "label");
    }

    /**
     * Selects only the columns we need from the original Dataset, dropping the rest.
     * The columns kept are the GISJOIN column, all feature columns, and the label column.
     * Filters by GISJOIN, keeping only the Rows which have a match.
     * @param inputDataset Original Dataset without modifications.
     * @param features List<String> of column names to use as features.
     * @param label String name of column to use as label
     * @param gisJoin GISJOIN identifying which spatial extent we want records for.
     * @return A new Dataset<Row> containing the GISJOIN, features, and label column after filtering.
     */
    public static Dataset<Row> selectColsAndFilterByGisJoin(Dataset<Row> inputDataset, List<String> features,
                                                            String label, String gisJoin) {
        // Select just the columns we want, discard the rest, then filter by the model's GISJOIN
        Seq<String> columns = desiredColumnsAsSeq(features, label);
        Dataset<Row> selected = inputDataset.select("GISJOIN", columns);
        return selected.filter(
                selected.col("GISJOIN").equalTo(gisJoin) // filter by GISJOIN
        ).withColumnRenamed(label, "label"); // Rename the chosen label column to "label"
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
     * @param gisJoin String spatial GISJOIN identifier to filter by
     * @return Dataset<Row> containing a "GISJOIN" column, vector "features" column, and "label" column.
     */
    public static Dataset<Row> loadAndProcessDataset(JavaSparkContext sparkContext, String collectionName,
                                                     List<String> features, String label, String gisJoin) {
        Dataset<Row> mongoCollectionDs = MongoSpark.load(sparkContext, createReadConfig(sparkContext, collectionName))
                .toDS(Row.class);

        Dataset<Row> selectedAndFilteredDs = selectColsAndFilterByGisJoin(mongoCollectionDs, features, label, gisJoin);
        return createFeaturesColumn(selectedAndFilteredDs, features);
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

            RegressionModel model = buildRegressionModel();
            model.train(trainSet); // Launches the Spark Model
            modelResponses.add(buildModelResponse(gisJoin, model));
        }
        return modelResponses;
    }

    /**
     * Builds a RegressionModel, as implemented by the concrete regression subclasses.
     * @return A RegressionModel concrete instance.
     */
    public abstract RegressionModel buildRegressionModel();

    /**
     * Builds a ModelResponse from the trained/tested RegressionModel, as implemented by the concrete
     * regression subclasses.
     * @param gisJoin The GISJOIN the ModelResponse is for
     * @param model The trained/tested RegressionModel instance
     * @return A ModelResponse to be sent back via gRPC
     */
    public abstract ModelResponse buildModelResponse(String gisJoin, RegressionModel model);
}
