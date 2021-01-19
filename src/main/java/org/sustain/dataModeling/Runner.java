package org.sustain.dataModeling;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setConf("spark.driver.extraJavaOptions",
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + 5005);
        sparkLauncher
                .setSparkHome(System.getenv("SPARK_HOME"))
                //.setAppResource("build/libs/sustain-census-grpc-1.0-SNAPSHOT.jar")
                .setAppResource("/home/menuka/github/sustain-census-grpc/build/libs/sustain-census-grpc-1.0-SNAPSHOT" +
                        ".jar")
                .addSparkArg("--master", "local")
                .setMainClass("org.sustain.dataModeling.Main")
                .startApplication();

    }
}