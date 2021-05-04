/* ========================================================
 * GBoostRegressionModel.java -
 *      Defines a generalized gradient boost regression model that can be
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
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
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
 * Provides an interface for building generalized Gradient Boost Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class GBoostRegressionModel {

    protected static final Logger log = LogManager.getLogger(GBoostRegressionModel.class);

    private Dataset<Row> mongoCollection;
    private String database, collection, mongoUri;
    private List<String> features;
    private String label, gisJoin;
    private final String queryField = "gis_join";
    private double rmse = 0.0;
    private double r2 = 0.0;

    // Loss function which GBT tries to minimize. (case-insensitive) Supported: "squared" (L2) and "absolute" (L1)
    // (default = squared)
    private String lossType = null;
    // Criterion used for information gain calculation. Supported values: "variance".
    private String impurity = null;
    // If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all";
    // if numTrees > 1 (forest) set to "onethird".
    private String featureSubsetStrategy = null;

    // Minimum number of instances each child must have after split. If a split causes the left or right child to have
    // fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    private Integer minInstancesPerNode = null;
    // Max number of iterations
    private Integer maxIter = null;
    // Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes).
    // (suggested value: 4)
    private Integer maxDepth = null;
    // Maximum number of bins used for splitting features. (suggested value: 100)
    private Integer maxBins = null;

    // Minimum information gain for a split to be considered at a tree node. default 0.0
    private Double minInfoGain = null;
    // Minimum fraction of the weighted sample count that each child must have after split. Should be in the
    // interval [0.0, 0.5). (default = 0.0)
    private Double minWeightFractionPerNode = null;
    // Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    private Double subsamplingRate = null;
    // Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator.
    // (default = 0.1)
    private Double stepSize = null;
    // Ratio of Training Data size to Test Data size . Range - (0, 1).
    private Double trainSplit = null;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private GBoostRegressionModel() {}

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
        cols.add(queryField);
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
    public boolean train() {

        log.info(">>> Building Gradient-Boosted Model for GISJoin {}...", this.gisJoin);
        Task trainTask = new Task(String.format("GBRModel train(%s)", this.gisJoin), 0);

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

        GBTRegressor gradientBoost = new GBTRegressor()
                .setFeaturesCol("features")
                .setLabelCol("label")
                .setLossType(this.lossType)
                .setImpurity(this.impurity)
                .setFeatureSubsetStrategy(this.featureSubsetStrategy)
                .setMinInstancesPerNode(this.minInstancesPerNode)
                .setMaxIter(this.maxIter)
                .setMaxDepth(this.maxDepth)
                .setMaxBins(this.maxBins)
                .setMinInfoGain(this.minInfoGain)
                .setMinWeightFractionPerNode(this.minWeightFractionPerNode)
                .setSubsamplingRate(this.subsamplingRate)
                .setStepSize(this.stepSize);

        GBTRegressionModel gbModel = gradientBoost.fit(trainSet);

        Dataset<Row> predictions = gbModel.transform(testSet).select("label", "prediction");
        RegressionMetrics metrics = new RegressionMetrics(predictions);

        this.rmse = metrics.rootMeanSquaredError();
        this.r2 = metrics.r2();

        trainTask.finish();
        log.info(">>> Finished building model for GISJoin: {}, Task: {}", this.gisJoin, trainTask);
        return true;
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

        GBoostRegressionModel model = new GBoostRegressionModel.GradientBoostRegressionBuilder()
                .forMongoCollection(MongoSpark.load(sparkContext, readConfig).toDF())
                .forGISJoin(gisJoin)
                .forFeatures(features)
                .forLabel(label)
                .build();

        model.train();
        log.info("Executed GBoostRegressionModel.main() successfully");
        sparkContext.close();
    }

    /**
     * Builder class for the GBoostRegressionModel object.
     */
    public static class GradientBoostRegressionBuilder implements ModelBuilder<GBoostRegressionModel> {

        private Dataset<Row>     mongoCollection;
        private List<String>     features;
        private String           gisJoin, label;

        // Model parameters and their defaults
        private String           lossType="squared", impurity="variance", featureSubsetStrategy="auto";
        private Integer          minInstancesPerNode=1, maxDepth=5, maxIterations=10, maxBins=32;
        private Double           minInfoGain=0.0, minWeightFractionPerNode=0.0, subsamplingRate=1.0, stepSize=0.1,
                                 trainSplit = 0.8;

        public GradientBoostRegressionBuilder forMongoCollection(Dataset<Row> mongoCollection) {
            this.mongoCollection = mongoCollection;
            return this;
        }

        public GradientBoostRegressionBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public GradientBoostRegressionBuilder forLabel(String label) {
            this.label = label;
            return this;
        }

        public GradientBoostRegressionBuilder forFeatures(List<String> features) {
            this.features = features;
            return this;
        }

        public GradientBoostRegressionBuilder withLossType(String lossType) {
            if (!lossType.isBlank()) {
                this.lossType = lossType;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withImpurity(String impurity) {
            if (!impurity.isBlank()) {
                this.impurity = impurity;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withFeatureSubsetStrategy(String featureSubsetStrategy) {
            if (!featureSubsetStrategy.isBlank()) {
                this.featureSubsetStrategy = featureSubsetStrategy;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMinInstancesPerNode(Integer minInstancesPerNode) {
            if (minInstancesPerNode != null && minInstancesPerNode >= 0 && minInstancesPerNode <= 10000) {
                this.minInstancesPerNode = minInstancesPerNode;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations <= 10000) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMaxDepth(Integer maxDepth) {
            if (maxDepth != null && maxDepth >= 0 && maxDepth <= 15) {
                this.maxDepth = maxDepth;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMaxBins(Integer maxBins) {
            if (maxBins != null && maxBins >= 2 && maxBins <= 100) {
                this.maxBins = maxBins;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMinInfoGain(Double minInfoGain) {
            if ((minInfoGain != null) && minInfoGain >= 0.0 && minInfoGain <= 1.0 ) {
                this.minInfoGain = minInfoGain;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withMinWeightFractionPerNode(Double minWeightFractionPerNode) {
            if (minWeightFractionPerNode != null && minWeightFractionPerNode >= 0.0 && minWeightFractionPerNode < 0.5) {
                this.minWeightFractionPerNode = minWeightFractionPerNode;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withSubsamplingRate(Double subsamplingRate) {
            if (subsamplingRate != null && subsamplingRate > 0.0 && subsamplingRate <= 1.0 ) {
                this.subsamplingRate = subsamplingRate;
            }
            return this;
        }

        public GradientBoostRegressionBuilder withStepSize(Double stepSize) {
            if (stepSize != null && stepSize > 0.0 && stepSize <= 1.0 )
                this.stepSize = stepSize;
            return this;
        }

        public GradientBoostRegressionBuilder withTrainSplit(Double trainSplit) {
            if (trainSplit != null && trainSplit > 0.0 && trainSplit < 1.0 )
                this.trainSplit = trainSplit;
            return this;
        }

        @Override
        public GBoostRegressionModel build() {
            GBoostRegressionModel model = new GBoostRegressionModel();
            model.mongoCollection = this.mongoCollection;
            model.gisJoin = this.gisJoin;
            model.features = this.features;
            model.label = this.label;
            model.lossType = this.lossType;
            model.impurity = this.impurity;
            model.featureSubsetStrategy = this.featureSubsetStrategy;
            model.minInstancesPerNode = this.minInstancesPerNode;
            model.maxDepth = this.maxDepth;
            model.maxIter = this.maxIterations;
            model.maxBins = this.maxBins;
            model.minInfoGain = this.minInfoGain;
            model.minWeightFractionPerNode = this.minWeightFractionPerNode;
            model.subsamplingRate = this.subsamplingRate;
            model.stepSize = this.stepSize;
            model.trainSplit = this.trainSplit;
            return model;
        }
    }

}