package org.sustain.modeling;


import org.apache.spark.ml.regression.RegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SustainRegressionModel {

    public abstract void train(Dataset<Row> trainingDataset);

    public abstract void test(Dataset<Row> testingDataset);

}
