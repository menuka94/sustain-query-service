/* ---------------------------------------------------------------------------------------------------------------------
 * LRModel.java -
 *      Defines a generalized linear regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Caleb Carlson
 * ------------------------------------------------------------------------------------------------------------------ */

package org.sustain.modeling;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class SustainLinearRegressionModel extends SustainRegressionModel {

    protected static final Logger log = LogManager.getLogger(SustainLinearRegressionModel.class);

    private LinearRegressionModel sparkLinearRegressionModel;

    private String           loss, solver;
    private Integer          aggregationDepth, maxIterations, totalIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance, rmse, r2, intercept;
    private List<Double>     coefficients, objectiveHistory;
    private Boolean          fitIntercept, setStandardization;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private SustainLinearRegressionModel() {}

    // Getters for training results

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
     * Trains a Linear Regression model for a single GISJoin.
     */
    public void train(Dataset<Row> trainingDataset) {

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
        this.sparkLinearRegressionModel = linearRegression.fit(trainingDataset);

        // Save training summary
        LinearRegressionTrainingSummary summary = this.sparkLinearRegressionModel.summary();

        // Add all coefficients to ArrayList
        this.coefficients = new ArrayList<>();
        double[] primitiveCoefficients = this.sparkLinearRegressionModel.coefficients().toArray();
        for (double d: primitiveCoefficients) {
            this.coefficients.add(d);
        }

        // Add all iteration RMSE values to objective history
        this.objectiveHistory = new ArrayList<>();
        double[] primitiveObjHistory = summary.objectiveHistory();
        for (double d: primitiveObjHistory) {
            this.objectiveHistory.add(d);
        }

        this.intercept = this.sparkLinearRegressionModel.intercept();
        this.totalIterations = summary.totalIterations();
        this.rmse = summary.rootMeanSquaredError();
        this.r2 = summary.r2();
    }

    @Override
    public void test(Dataset<Row> testingDataset) {

    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {}

    /**
     * Builder class for the LRModel object.
     */
    public static class LinearRegressionModelBuilder implements ModelBuilder<SustainLinearRegressionModel> {

        // Model parameters and their defaults
        private String           loss="squaredError", solver="auto";
        private Integer          aggregationDepth=2, maxIterations=10;
        private Double           elasticNetParam=0.0, epsilon=1.35, regularizationParam=0.5, convergenceTolerance=1E-6;
        private Boolean          fitIntercept=true, setStandardization=true;

        public LinearRegressionModelBuilder withLoss(String loss) {
            if (!loss.isBlank()) {
                this.loss = loss;
            }
            return this;
        }

        public LinearRegressionModelBuilder withSolver(String solver) {
            if (!solver.isBlank()) {
                this.solver = solver;
            }
            return this;
        }

        public LinearRegressionModelBuilder withAggregationDepth(Integer aggregationDepth) {
            if (aggregationDepth != null && aggregationDepth >= 2 && aggregationDepth <= 10) {
                this.aggregationDepth = aggregationDepth;
            }
            return this;
        }

        public LinearRegressionModelBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations < 100) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public LinearRegressionModelBuilder withElasticNetParam(Double elasticNetParam) {
            if ((elasticNetParam != null) && elasticNetParam >= 0.0 && elasticNetParam <= 1.0 ) {
                this.elasticNetParam = elasticNetParam;
            }
            return this;
        }

        public LinearRegressionModelBuilder withEpsilon(Double epsilon) {
            if (epsilon != null && epsilon > 1.0 && epsilon <= 10.0) {
                this.epsilon = epsilon;
            }
            return this;
        }

        public LinearRegressionModelBuilder withRegularizationParam(Double regularizationParam) {
            if (regularizationParam != null && regularizationParam >= 0.0 && regularizationParam <= 10.0 ) {
                this.regularizationParam = regularizationParam;
            }
            return this;
        }

        public LinearRegressionModelBuilder withTolerance(Double convergenceTolerance) {
            if (convergenceTolerance != null && convergenceTolerance >= 0.0 && convergenceTolerance <= 10.0 )
            this.convergenceTolerance = convergenceTolerance;
            return this;
        }

        public LinearRegressionModelBuilder withFitIntercept(Boolean fitIntercept) {
            if (fitIntercept != null) {
                this.fitIntercept = fitIntercept;
            }
            return this;
        }

        public LinearRegressionModelBuilder withStandardization(Boolean setStandardization) {
            if (setStandardization != null) {
                this.setStandardization = setStandardization;
            }
            return this;
        }

        @Override
        public SustainLinearRegressionModel build() {
            SustainLinearRegressionModel model = new SustainLinearRegressionModel();
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
