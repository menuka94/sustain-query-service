package org.sustain.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.SparkManager;
import org.sustain.util.Constants;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SustainServer {

    private static final Logger log = LogManager.getLogger(SustainServer.class);
    private static final String[] sparkJarPaths = {
        "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
        "build/libs/spark-core_2.12-3.0.1.jar",
        "build/libs/spark-mllib_2.12-3.0.1.jar",
        "build/libs/spark-sql_2.12-3.0.1.jar",
        "build/libs/bson-4.0.5.jar",
        "build/libs/mongo-java-driver-3.12.5.jar"
    };

    private Server server;
    private SparkManager sparkManager;

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("\nKJ-108 BRANCH\n");
        logEnvironment();

        final SustainServer server = new SustainServer();
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Logs the environment variables that the server was started with.
     */
    public static void logEnvironment() {
        if (Constants.Kubernetes.KUBERNETES_ENV) {
            log.info("Running within a Kubernetes Pod (KUBERNETES_ENV = true)");
            log.info("\n\n--- Kubernetes Environment ---\n" +
                            "NODE_HOSTNAME: {}\n" +
                            "POD_NAME: {}\n" +
                            "POD_IP: {}\n",
                    Constants.Kubernetes.NODE_HOSTNAME, Constants.Kubernetes.POD_NAME, Constants.Kubernetes.POD_IP
            );
        } else {
            log.info("Running directly on host OS");
        }

        log.info("\n\n--- Server Environment ---\n" +
                "JAVA_HOME: {}\n" +
                "SERVER_HOST: {}\n" +
                "SERVER_PORT: {}\n" +
                "\n\n--- Database Environment ---\n" +
                "DB_HOST: {}\n" +
                "DB_PORT: {}\n" +
                "DB_NAME: {}\n" +
                "\n\n--- Spark Environment ---\n" +
                "SPARK_MASTER: {}\n" +
                "SPARK_DRIVER_PORT: {}\n" +
                "SPARK_DRIVER_BLOCKMANAGER_PORT: {}\n" +
                "SPARK_DRIVER_UI_PORT: {}\n" +
                "SPARK_DEFAULT_EXECUTOR_PORT: {}\n" +
                "EXECUTOR_CORES: {}\n" +
                "EXECUTOR_MEMORY: {}\n" +
                "INITIAL_EXECUTORS: {}\n" +
                "\n\n--- Druid Environment ---\n" +
                "QUERY_HOST: {}\n" +
                "QUERY_POST: {}\n",
                Constants.Java.HOME,
                Constants.Server.HOST, Constants.Server.PORT,
                Constants.DB.HOST, Constants.DB.PORT, Constants.DB.NAME,
                Constants.Spark.MASTER, Constants.Spark.DRIVER_PORT, Constants.Spark.DRIVER_BLOCKMANAGER_PORT,
                Constants.Spark.DRIVER_UI_PORT, Constants.Spark.DEFAULT_EXECUTOR_PORT,
                Constants.Spark.EXECUTOR_CORES, Constants.Spark.EXECUTOR_MEMORY, Constants.Spark.INITIAL_EXECUTORS,
                Constants.Druid.QUERY_HOST, Constants.Druid.QUERY_PORT
        );
    }

    public void start() throws IOException {
        // initialize SparkManager
        sparkManager = new SparkManager(Constants.Spark.MASTER);

        for (String jar: sparkJarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkManager.addJar(jar);
        }

        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
            .addService(new JsonProxyService(sparkManager))
            .addService(new SustainService(sparkManager))
            .build().start();
        log.info("Server started, listening on " + port);

        // Shutdown hook called when Ctrl+C is pressed
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

                try {
                    SustainServer.this.stop();
                } catch (InterruptedException e) {
                    log.error("Error in stopping the server");
                    e.printStackTrace();
                }
                log.warn("Server is shutting down");

        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdownNow() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
