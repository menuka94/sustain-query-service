package org.sustain.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProfilingUtil {
    private static final Logger log = LogManager.getLogger(ProfilingUtil.class);
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private static BufferedWriter bw;
    private static boolean newRun = true;

    static {
        String outputFile = System.getenv("HOME") + File.separator + "sustain-profiling.out";
        try {
            bw = new BufferedWriter(new FileWriter(outputFile, true));
        } catch (IOException e) {
            log.error("Error opening profiling output file: {}", e.getMessage());
            e.printStackTrace();
        }

    }

    public static void calculateTimeDiff(long time1, long time2, String label) {
        double timeTaken = (time2 - time1) / 1000.0;
        String logLine = String.format("%s: %f ms", label, timeTaken);
        log.info(logLine);
        writeToFile(logLine);
    }

    public static void evaluateClusteringModel(Dataset<Row> evaluateDF, String modelName, String additionalInfo) {
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(evaluateDF);
        String logLine;
        if (additionalInfo.isEmpty()) {
            logLine = String.format("%s: Silhouette with squared euclidean distance = %f", modelName, silhouette);
        } else {
            logLine = String.format("%s: Silhouette with squared euclidean distance = %f -- %s", modelName,
                silhouette, additionalInfo);
        }
        log.info(logLine);
        writeToFile(logLine);
    }

    public static void evaluateClusteringModel(Dataset<Row> evaluateDF, String modelName) {
        evaluateClusteringModel(evaluateDF, modelName, "");
    }

    public static void writeToFile(String line) {
        try {
            if (bw != null) {
                line = dateTimeFormatter.format(LocalDateTime.now()) + ": " + line;
                bw.write(line);
                bw.newLine();
                if (newRun) {
                    newRun = false;
                    bw.write("-----------------------------------------------");
                    bw.newLine();
                }
                bw.flush();
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
