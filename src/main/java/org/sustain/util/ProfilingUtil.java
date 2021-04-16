package org.sustain.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ProfilingUtil {
    private static final Logger log = LogManager.getLogger(ProfilingUtil.class);
    private static BufferedWriter bw;

    static {
        try {
            bw = new BufferedWriter(new FileWriter("profiling.txt", true));
        } catch (IOException e) {
            log.error("Error opening profiling output file: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void calculateTimeDiff(long time1, long time2, String label) {
        double timeTaken = (time2 - time1) / 1000.0;
        String logLine = String.format("%s: %f", label, timeTaken);
        log.info(logLine);
        try {
            writeToFile(logLine);
        } catch (IOException e) {
            log.error("Error writing to file: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void evaluateClusteringModel(Dataset<Row> evaluateDF, String modelName) {
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(evaluateDF);
        String logLine = String.format("%s: Silhouette with squared euclidean distance = %f", modelName, silhouette);
        log.info(logLine);
        try {
            writeToFile(logLine);
        } catch (IOException e) {
            log.error("Error writing to file: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private static void writeToFile(String line) throws IOException {
        if (bw != null) {
            bw.write(line);
            bw.newLine();
            bw.flush();
        }
    }
}
