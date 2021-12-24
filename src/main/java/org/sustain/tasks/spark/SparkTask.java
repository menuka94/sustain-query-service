package org.sustain.tasks.spark;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkTask<T> {

    T execute(JavaSparkContext sparkContext) throws Exception;

}
