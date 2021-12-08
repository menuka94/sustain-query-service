package org.sustain;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.sustain.handlers.tasks.SparkTask;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SparkManager {
    protected ExecutorService executorService; 
    protected List<String> jars;
    private String sparkMaster;

    public SparkManager(String sparkMaster) {
        this.sparkMaster = sparkMaster;
        this.jars = new ArrayList<>();
        this.executorService = Executors.newCachedThreadPool();
    }

    public void addJar(String jar) {
        this.jars.add(jar);
    }

    protected void cancel(String jobGroup) {
        // initialize spark session
        SparkSession sparkSession = getOrCreateSparkSession();
        JavaSparkContext sparkContext = 
            new JavaSparkContext(sparkSession.sparkContext());

        // cancel job group
        sparkContext.cancelJobGroup(jobGroup);
    }

    protected SparkSession getOrCreateSparkSession() {
        // get or create SparkSession
        SparkSession sparkSession = SparkSession.builder()
            .master(this.sparkMaster)
            .appName("sustain-query-service-" + Constants.Server.HOST)
            .config("spark.executor.cores",
                Constants.Spark.EXECUTOR_CORES)
            .config("spark.executor.memory",
                Constants.Spark.EXECUTOR_MEMORY)
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            .config("spark.dynamicAllocation.initialExecutors",
                Constants.Spark.INITIAL_EXECUTORS)
            .config("spark.dynamicAllocation.minExecutors",
                Constants.Spark.MIN_EXECUTORS)
            .config("spark.dynamicAllocation.maxExecutors",
                Constants.Spark.MAX_EXECUTORS)
            .config("spark.dynamicAllocation.schedulerBacklogTimeout",
                Constants.Spark.BACKLOG_TIMEOUT)
            .config("spark.dynamicAllocation.executorIdleTimeout",
                Constants.Spark.IDLE_TIMEOUT)
            .config("mongodb.keep_alive_ms", "100000")
            .getOrCreate();

        // if they don't exist - add JARs to SparkContext
        JavaSparkContext sparkContext =
            new JavaSparkContext(sparkSession.sparkContext());
        for (String jar : this.jars) {
            if (!sparkContext.jars().contains(jar)) {
                sparkContext.addJar(jar);
            }
        }

        return sparkSession;
    }

    public <T> Future<T> submit(SparkTask<T> sparkTask, String jobGroup) {
        return this.executorService.submit(() -> {
            // initialize spark session
            SparkSession sparkSession = getOrCreateSparkSession();
            JavaSparkContext sparkContext = 
                new JavaSparkContext(sparkSession.sparkContext());

            // set job group so all jobs submitted from this thread
            // share a common id, also set interruptOnCancel
            sparkContext.setJobGroup(jobGroup, "", true);

            try {
                // execute spark task
                return sparkTask.execute(sparkContext);
            } catch (Exception e) {
                // if this callable is interrupted
                // -> cancel spark jobs for this group
                sparkContext.cancelJobGroup(jobGroup);
                throw e;
            }
        });
    }
}
