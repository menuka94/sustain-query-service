package org.sustain.modeling;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class RegressionModel {

    public abstract void train(Dataset<Row> trainingDataset);

}
