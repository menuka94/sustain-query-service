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
package org.sustain.modeling.spark.regression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.modeling.ModelBuilder;

/**
 * Provides an interface for building generalized Gradient Boost Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class SustainGradientBoostRegressionModel extends SustainRegressionModel {

    protected static final Logger log = LogManager.getLogger(SustainGradientBoostRegressionModel.class);

    private GBTRegressionModel sparkGradientBoostRegressionModel;

    // Root Mean Squared Error
    private Double rmse;

    // R^2 (r-squared)
    private Double r2;

    // Loss function which GBT tries to minimize. (case-insensitive) Supported: "squared" (L2) and "absolute" (L1)
    // (default = squared)
    private String lossType;

    // Criterion used for information gain calculation. Supported values: "variance".
    private String impurity;

    // If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all";
    // If numTrees > 1 (forest) set to "onethird".
    private String featureSubsetStrategy;

    // Minimum number of instances each child must have after split. If a split causes the left or right child to have
    // fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    private Integer minInstancesPerNode;

    // Max number of iterations
    private Integer maxIter;

    // Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes).
    // (suggested value: 4)
    private Integer maxDepth;

    // Maximum number of bins used for splitting features. (suggested value: 100)
    private Integer maxBins;

    // Minimum information gain for a split to be considered at a tree node. default 0.0
    private Double minInfoGain;

    // Minimum fraction of the weighted sample count that each child must have after split. Should be in the
    // interval [0.0, 0.5). (default = 0.0)
    private Double minWeightFractionPerNode;

    // Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    private Double subsamplingRate;

    // Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator.
    // (default = 0.1)
    private Double stepSize;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private SustainGradientBoostRegressionModel() {}

    public double getRmse() {
        return rmse;
    }

    public double getR2() {
        return r2;
    }

    @Override
    public void train(Dataset<Row> trainingDataset) {
        // Build org.apache.spark.ml.regression.GBTRegressor using requested parameters
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

        // Fit to training set
        this.sparkGradientBoostRegressionModel = gradientBoost.fit(trainingDataset);
    }

    @Override
    public void test(Dataset<Row> testingDataset) {
        // Adds the "prediction" column
        Dataset<Row> predictions = this.sparkGradientBoostRegressionModel.transform(testingDataset)
                .select("label", "prediction");

        // Take metrics
        RegressionMetrics metrics = new RegressionMetrics(predictions);
        this.rmse = metrics.rootMeanSquaredError();
        this.r2 = metrics.r2();
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {

    }

    /**
     * Builder class for the GBoostRegressionModel object.
     */
    public static class GradientBoostRegressionModelBuilder implements ModelBuilder<SustainGradientBoostRegressionModel> {

        // Model parameters and their defaults
        private String           lossType="squared", impurity="variance", featureSubsetStrategy="auto";
        private Integer          minInstancesPerNode=1, maxDepth=5, maxIterations=10, maxBins=32;
        private Double           minInfoGain=0.0, minWeightFractionPerNode=0.0, subsamplingRate=1.0, stepSize=0.1;

        public GradientBoostRegressionModelBuilder withLossType(String lossType) {
            if (!lossType.isBlank()) {
                this.lossType = lossType;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withImpurity(String impurity) {
            if (!impurity.isBlank()) {
                this.impurity = impurity;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withFeatureSubsetStrategy(String featureSubsetStrategy) {
            if (!featureSubsetStrategy.isBlank()) {
                this.featureSubsetStrategy = featureSubsetStrategy;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMinInstancesPerNode(Integer minInstancesPerNode) {
            if (minInstancesPerNode != null && minInstancesPerNode >= 0 && minInstancesPerNode <= 10000) {
                this.minInstancesPerNode = minInstancesPerNode;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations <= 10000) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMaxDepth(Integer maxDepth) {
            if (maxDepth != null && maxDepth >= 0 && maxDepth <= 15) {
                this.maxDepth = maxDepth;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMaxBins(Integer maxBins) {
            if (maxBins != null && maxBins >= 2 && maxBins <= 100) {
                this.maxBins = maxBins;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMinInfoGain(Double minInfoGain) {
            if ((minInfoGain != null) && minInfoGain >= 0.0 && minInfoGain <= 1.0 ) {
                this.minInfoGain = minInfoGain;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withMinWeightFractionPerNode(Double minWeightFractionPerNode) {
            if (minWeightFractionPerNode != null && minWeightFractionPerNode >= 0.0 && minWeightFractionPerNode < 0.5) {
                this.minWeightFractionPerNode = minWeightFractionPerNode;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withSubsamplingRate(Double subsamplingRate) {
            if (subsamplingRate != null && subsamplingRate > 0.0 && subsamplingRate <= 1.0 ) {
                this.subsamplingRate = subsamplingRate;
            }
            return this;
        }

        public GradientBoostRegressionModelBuilder withStepSize(Double stepSize) {
            if (stepSize != null && stepSize > 0.0 && stepSize <= 1.0 )
                this.stepSize = stepSize;
            return this;
        }

        @Override
        public SustainGradientBoostRegressionModel build() {
            SustainGradientBoostRegressionModel model = new SustainGradientBoostRegressionModel();
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
            return model;
        }
    }

}