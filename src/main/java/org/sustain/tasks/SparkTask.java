package org.sustain.tasks;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkTask<T> {

    T execute(JavaSparkContext sparkContext) throws Exception;

}
