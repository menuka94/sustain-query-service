package org.sustain;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SparkManager {
    protected ExecutorService executorService; 
    protected List<String> jars;
    private String sparkMaster;

    public SparkManager(String sparkMaster, int threadCount) {
        this.sparkMaster = sparkMaster;
        this.jars = new ArrayList();
        this.executorService = Executors.newFixedThreadPool(threadCount);
    }

    public void addJar(String jar) {
        this.jars.add(jar);
    }

    protected void cancel(String jobGroup) throws Exception {
        // initialize spark session
        SparkSession sparkSession = getOrCreateSparkSession();
        JavaSparkContext sparkContext = 
            new JavaSparkContext(sparkSession.sparkContext());

        // cancel job group
        sparkContext.cancelJobGroup(jobGroup);
    }

    protected SparkSession getOrCreateSparkSession() throws Exception {
        // get or create SparkSession
        SparkSession sparkSession = SparkSession.builder()
            .master(this.sparkMaster)
            .appName("ConcurrentSpark")
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

    public <T> Future<T> submit(SparkTask<T> sparkTask,
            String jobGroup) throws Exception {
        Future<T> future = this.executorService.submit(() -> {
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

        return future;
    }
}
