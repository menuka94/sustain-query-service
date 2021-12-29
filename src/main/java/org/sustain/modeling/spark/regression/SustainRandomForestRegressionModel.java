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
package org.sustain.modeling.spark.regression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.modeling.ModelBuilder;

import java.util.List;

/**
 * Provides an interface for building generalized Random Forest Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class SustainRandomForestRegressionModel extends SustainRegressionModel {

    protected static final Logger log = LogManager.getLogger(SustainRandomForestRegressionModel.class);

    private RandomForestRegressionModel sparkRandomForestRegressionModel;

    // Root Mean Squared Error
    private Double rmse;

    // R^2 (r-squared)
    private Double r2;

    // Criterion used for information gain calculation. Supported values: "variance".
    private String impurity;

    // Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird".
    // If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all";
    // if numTrees > 1 (forest) set to "onethird".
    private String featureSubsetStrategy;

    // Minimum number of instances each child must have after split. If a split causes the left or right child to have
    // fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    private Integer minInstancesPerNode;

    // Number of trees to train (at least 1). If 1, then no bootstrapping is used. If greater than 1, then
    // bootstrapping is done.
    private Integer numTrees;

    // Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes).
    // (suggested value: 4)
    private Integer maxDepth;

    // Maximum number of bins used for splitting features. (suggested value: 100)
    private Integer maxBins;

    // Minimum information gain for a split to be considered at a tree node. default 0.0
    private Double minInfoGain;

    // Minimum fraction of the weighted sample count that each child must have after split. Should be in the interval [0.0, 0.5). (default = 0.0)
    private Double minWeightFractionPerNode;

    // Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    private Double subsamplingRate;

    // Whether bootstrap samples are used when building trees.
    private Boolean isBootstrap;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private SustainRandomForestRegressionModel() {}

    public double getRmse() {
        return rmse;
    }

    public double getR2() {
        return r2;
    }

    @Override
    public void train(Dataset<Row> trainingDataset) {

        RandomForestRegressor sparkRandomForestRegressor = new RandomForestRegressor()
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

        this.sparkRandomForestRegressionModel = sparkRandomForestRegressor.fit(trainingDataset);
    }

    @Override
    public void test(Dataset<Row> testingDataset) {
        Dataset<Row> predictions = this.sparkRandomForestRegressionModel.transform(testingDataset)
                .select("label", "prediction");
        RegressionMetrics metrics = new RegressionMetrics(predictions);

        this.rmse = metrics.rootMeanSquaredError();
        this.r2 = metrics.r2();
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {}

    /**
     * Builder class for the RFRegressionModel object.
     */
    public static class RFRegressionBuilder implements ModelBuilder<SustainRandomForestRegressionModel> {

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

        public RFRegressionBuilder forGisJoin(String gisJoin) {
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
        public SustainRandomForestRegressionModel build() {
            SustainRandomForestRegressionModel model = new SustainRandomForestRegressionModel();
            model.impurity = this.impurity;
            model.numTrees = this.numTrees;
            model.featureSubsetStrategy = this.featureSubsetStrategy;
            model.minInstancesPerNode = this.minInstancesPerNode;
            model.maxDepth = this.maxDepth;
            model.maxBins = this.maxBins;
            model.minInfoGain = this.minInfoGain;
            model.minWeightFractionPerNode = this.minWeightFractionPerNode;
            model.subsamplingRate = this.subsamplingRate;
            model.isBootstrap = this.isBootstrap;
            return model;
        }
    }

}