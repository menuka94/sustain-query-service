/* ========================================================
 * RFRegressionModel.java -
 *      Defines a generalized random forest regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Saptashwa Mitra
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ======================================================== */
package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sustain.util.Constants;
import org.sustain.util.Task;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides an interface for building generalized Random Forest Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class RFRegressionModel {

    protected static final Logger log = LogManager.getLogger(RFRegressionModel.class);

    private Dataset<Row> mongoCollection;
    private String database, collection, mongoUri;
    private List<String> features;
    private String label, gisJoin;
    private final String queryField = "gis_join";
    private double rmse = 0.0;
    private double r2 = 0.0;

    // MODEL PARAMETERS
    // Criterion used for information gain calculation. Supported values: "variance".
    private String impurity = null;
    // Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird".
    // If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all";
    // if numTrees > 1 (forest) set to "onethird".
    private String featureSubsetStrategy = null;

    // Minimum number of instances each child must have after split. If a split causes the left or right child to have
    // fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    private Integer minInstancesPerNode = null;
    // Number of trees to train (at least 1). If 1, then no bootstrapping is used. If greater than 1, then
    // bootstrapping is done.
    private Integer numTrees = null;
    // Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes).
    // (suggested value: 4)
    private Integer maxDepth = null;
    // Maximum number of bins used for splitting features. (suggested value: 100)
    private Integer maxBins = null;

    // Minimum information gain for a split to be considered at a tree node. default 0.0
    private Double minInfoGain = null;
    // Minimum fraction of the weighted sample count that each child must have after split. Should be in the interval [0.0, 0.5). (default = 0.0)
    private Double minWeightFractionPerNode = null;
    // Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    private Double subsamplingRate = null;
    // Ratio of Training Data size to Test Data size . Range - (0, 1).
    private Double trainSplit = 0.8;

    // Whether bootstrap samples are used when building trees.
    private Boolean isBootstrap = null;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private RFRegressionModel() {}

    public String getGisJoin() {
        return gisJoin;
    }

    public double getRmse() {
        return rmse;
    }

    public double getR2() {
        return r2;
    }

    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add(this.queryField);
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
     * Creates Spark context and trains the distributed model
     */
    public Boolean train() {

        // Begin a profiling task for how long it takes to train this gisJoin
        log.info(">>> Building Random-Forest model for GISJoin {}", this.gisJoin);
        Task trainTask = new Task(String.format("RFModel train(%s)", this.gisJoin), 0);

        // Select just the columns we want, discard the rest, then filter by the model's GISJoin
        Dataset<Row> selected = this.mongoCollection.select("_id", desiredColumns());
        Dataset<Row> gisDataset = selected.filter(selected.col(this.queryField).equalTo(this.gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"

        if (gisDataset.count() == 0) {
            log.info(">>> Dataset for GISJoin {} is empty!", this.gisJoin);
            return false;
        }

        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(this.features.toArray(new String[0]))
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);

        Dataset<Row>[] splits = mergedDataset.randomSplit(new double[]{this.trainSplit , 1.0 - this.trainSplit});
        Dataset<Row> trainSet = splits[0]; Dataset<Row> testSet  = splits[1];

        RandomForestRegressor randomForest = new RandomForestRegressor()
                .setFeaturesCol("features")
                .setLabelCol("label")
                .setImpurity(this.impurity)
                .setFeatureSubsetStrategy(this.featureSubsetStrategy)
                .setMinInstancesPerNode(this.minInstancesPerNode)
                .setNumTrees(this.numTrees)
                .setMaxDepth(this.maxDepth)
                .setMaxBins(this.maxBins)
                .setMinInfoGain(this.minInfoGain)
                .setMinWeightFractionPerNode(this.minWeightFractionPerNode)
                .setSubsamplingRate(this.subsamplingRate)
                .setBootstrap(this.isBootstrap);

        RandomForestRegressionModel rfModel = randomForest.fit(trainSet);
        Dataset<Row> predictions = rfModel.transform(testSet).select("label", "prediction");
        RegressionMetrics metrics = new RegressionMetrics(predictions);

        this.rmse = metrics.rootMeanSquaredError();
        this.r2 = metrics.r2();

        trainTask.finish();
        log.info(">>> Finished building model for GISJoin: {}, Task: {}", this.gisJoin, trainTask);
        return true;
    }

    /**
     * Injecting user-defined parameters into model
     * @param rf - Random Forest Regression model Object
     */
    private void ingestParameters(RandomForestRegressor rf) {
        if (this.isBootstrap != null) {
            rf.setBootstrap(this.isBootstrap);
        }
        if (this.subsamplingRate != null) {
            rf.setSubsamplingRate(this.subsamplingRate);
        }
        if (this.numTrees != null) {
            rf.setNumTrees(this.numTrees);
        }
        if (this.featureSubsetStrategy != null) {
            rf.setFeatureSubsetStrategy(this.featureSubsetStrategy);
        }
        if (this.impurity != null) {
            rf.setImpurity(this.impurity);
        }
        if (this.maxDepth != null) {
            rf.setMaxDepth(this.maxDepth);
        }
        if (this.maxBins != null) {
            rf.setMaxBins(this.maxBins);
        }

        if (this.minInfoGain != null) {
            rf.setMinInfoGain(this.minInfoGain);
        }

        if (this.minInstancesPerNode != null) {
            rf.setMinInstancesPerNode(this.minInstancesPerNode);
        }

        if (this.minWeightFractionPerNode != null) {
            rf.setMinWeightFractionPerNode(this.minWeightFractionPerNode);
        }

    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {
        List<String> features = Collections.singletonList("timestamp");
        String label = "min_eastward_wind";
        String gisJoin = "G0100290";
        String collection = "macav2";

        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Linear Regression Model")
                .config("spark.mongodb.input.uri", String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT))
                .config("spark.mongodb.input.database", Constants.DB.NAME)
                .config("spark.mongodb.input.collection", collection)
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        RFRegressionModel model = new RFRegressionModel.RFRegressionBuilder()
                .forMongoCollection(MongoSpark.load(sparkContext, readConfig).toDF())
                .forGISJoin(gisJoin)
                .forFeatures(features)
                .forLabel(label)
                .build();

        model.train();
        log.info("Executed RFRegressionModel.main() successfully");
        sparkContext.close();
    }

    /**
     * Builder class for the RFRegressionModel object.
     */
    public static class RFRegressionBuilder implements ModelBuilder<RFRegressionModel> {

        private Dataset<Row>     mongoCollection;
        private List<String>     features;
        private String           gisJoin, label;

        // Model parameters and their defaults
        private String           impurity="variance", featureSubsetStrategy="auto";
        private Integer          minInstancesPerNode=1, numTrees=20, maxDepth=5, maxBins=32;
        private Double           minInfoGain=0.0, minWeightFractionPerNode=0.0, subsamplingRate=1.0, trainSplit = 0.8;
        private Boolean          isBootstrap=false;

        public RFRegressionBuilder forMongoCollection(Dataset<Row> mongoCollection) {
            this.mongoCollection = mongoCollection;
            return this;
        }

        public RFRegressionBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public RFRegressionBuilder forLabel(String label) {
            this.label = label;
            return this;
        }

        public RFRegressionBuilder forFeatures(List<String> features) {
            this.features = features;
            return this;
        }

        public RFRegressionBuilder withImpurity(String impurity) {
            if (!impurity.isBlank()) {
                this.impurity = impurity;
            }
            return this;
        }

        public RFRegressionBuilder withFeatureSubsetStrategy(String featureSubsetStrategy) {
            if (!featureSubsetStrategy.isBlank()) {
                this.featureSubsetStrategy = featureSubsetStrategy;
            }
            return this;
        }

        public RFRegressionBuilder withMinInstancesPerNode(Integer minInstancesPerNode) {
            if (minInstancesPerNode != null && minInstancesPerNode >= 0 && minInstancesPerNode <= 10000) {
                this.minInstancesPerNode = minInstancesPerNode;
            }
            return this;
        }

        public RFRegressionBuilder withNumTrees(Integer numTrees) {
            if (numTrees != null && numTrees >= 1 && numTrees <= 1000) {
                this.numTrees = numTrees;
            }
            return this;
        }

        public RFRegressionBuilder withMaxDepth(Integer maxDepth) {
            if (maxDepth != null && maxDepth >= 0 && maxDepth <= 15) {
                this.maxDepth = maxDepth;
            }
            return this;
        }

        public RFRegressionBuilder withMaxBins(Integer maxBins) {
            if (maxBins != null && maxBins >= 2 && maxBins <= 100) {
                this.maxBins = maxBins;
            }
            return this;
        }

        public RFRegressionBuilder withMinInfoGain(Double minInfoGain) {
            if ((minInfoGain != null) && minInfoGain >= 0.0 && minInfoGain <= 1.0 ) {
                this.minInfoGain = minInfoGain;
            }
            return this;
        }

        public RFRegressionBuilder withMinWeightFractionPerNode(Double minWeightFractionPerNode) {
            if (minWeightFractionPerNode != null && minWeightFractionPerNode >= 0.0 && minWeightFractionPerNode < 0.5) {
                this.minWeightFractionPerNode = minWeightFractionPerNode;
            }
            return this;
        }

        public RFRegressionBuilder withSubsamplingRate(Double subsamplingRate) {
            if (subsamplingRate != null && subsamplingRate > 0.0 && subsamplingRate <= 1.0 ) {
                this.subsamplingRate = subsamplingRate;
            }
            return this;
        }

        public RFRegressionBuilder withTrainSplit(Double trainSplit) {
            if (trainSplit != null && trainSplit > 0.0 && trainSplit < 1.0 )
                this.trainSplit = trainSplit;
            return this;
        }

        public RFRegressionBuilder withIsBootstrap(Boolean isBootstrap) {
            if (isBootstrap != null)
                this.isBootstrap = isBootstrap;
            return this;
        }

        @Override
        public RFRegressionModel build() {
            RFRegressionModel model = new RFRegressionModel();
            model.mongoCollection = this.mongoCollection;
            model.gisJoin = this.gisJoin;
            model.features = this.features;
            model.label = this.label;
            model.impurity = this.impurity;
            model.numTrees = this.numTrees;
            model.featureSubsetStrategy = this.featureSubsetStrategy;
            model.minInstancesPerNode = this.minInstancesPerNode;
            model.maxDepth = this.maxDepth;
            model.maxBins = this.maxBins;
            model.minInfoGain = this.minInfoGain;
            model.minWeightFractionPerNode = this.minWeightFractionPerNode;
            model.subsamplingRate = this.subsamplingRate;
            model.trainSplit = this.trainSplit;
            model.isBootstrap = this.isBootstrap;
            return model;
        }
    }

}