package org.sustain.dataModeling;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        //SparkConf sparkConf = new SparkConf().setAppName("JavaSparkTest").setMaster("local");
        //JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        System.out.println("Hello World from Spark!");
    }
}
