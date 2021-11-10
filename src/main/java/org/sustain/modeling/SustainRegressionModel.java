package org.sustain.modeling;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SustainRegressionModel {



    public abstract boolean train(Dataset<Row> collection);

}
