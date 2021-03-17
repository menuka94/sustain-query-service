package org.sustain;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkTask<T> {
    public abstract T execute(JavaSparkContext sparkContext) throws Exception; 
}
