package org.sustain.dataModeling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;

public class JobLauncher implements SparkAppHandle.Listener {
    private static final Logger log = LogManager.getLogger(JobLauncher.class);

    public SparkAppHandle launchJob(String mainClass) throws IOException {
        String appResource = "app.jar";
        String sparkMaster = "spark://menuka-HP:7077";
        String deployMode = "client";

        SparkAppHandle handle = new SparkLauncher()
                .setAppResource(appResource)
                .setMainClass(mainClass)
                .setMainClass(sparkMaster)
                .setDeployMode(deployMode)
                .redirectOutput(new File("spark-output"))
                .redirectError(new File("spark-error"))
                .startApplication(this);

        log.info("Launched [" + mainClass + "] from [" + appResource + "] State [" + handle.getState() + "]");

        return handle;
    }

    @Override
    public void stateChanged(SparkAppHandle handle) {
        log.info("Spark App Id [" + handle.getAppId() + "] State Changed. State [" + handle.getState() + "]");
    }

    @Override
    public void infoChanged(SparkAppHandle handle) {
        log.info("Spark App Id [" + handle.getAppId() + "] Info Changed.  State [" + handle.getState() + "]");
    }
}
