/* ========================================================
 * RFRegressionModel.java -
 *      Defines a generalized random forest regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Saptashwa Mitra
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ======================================================== */
package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;
/**
 * Provides an interface for building generalized Random Forest Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class RFRegressionModel {

    // DATABASE PARAMETERS
    protected static final Logger log = LogManager.getLogger(RFRegressionModel.class);
    private String[] features;
    private String label, gisJoin;


    private JavaSparkContext sparkContext;

    // MODEL PARAMETERS
    //Whether bootstrap samples are used when building trees.
    private Boolean isBootstrap = null;
    //Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    private Double subsamplingRate = null;
    //Number of trees to train (at least 1). If 1, then no bootstrapping is used. If greater than 1, then bootstrapping is done.
    private Integer numTrees = 1;
    // Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird".
    // If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "onethird".
    private String featureSubsetStrategy = null; //auto/all/sqrt/log2/onethird
    //Criterion used for information gain calculation. Supported values: "variance".
    private String impurity = null;
    //maxDepth - Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes). (suggested value: 4)
    private Integer maxDepth = null;
    //maxBins - Maximum number of bins used for splitting features. (suggested value: 100)
    private Integer maxBins = null;
    private Double trainSplit = 0.7d;
    String errorType = "rmse";
    String queryField = "gis_join";
    //String queryField = "countyName";

    double rmse = 0.0;
    private double r2 = 0.0;

    public double getR2() {
        return r2;
    }

    public void setR2(double r2) {
        this.r2 = r2;
    }

    public double getRmse() {
        return rmse;
    }

    public void setRmse(double rmse) {
        this.rmse = rmse;
    }

    public void setTrainSplit(Double trainSplit) {
        this.trainSplit = trainSplit;
    }

    public RFRegressionModel(String master, String mongoUri, String database, String collection, String gisJoin) {
        log.info("Random Forest constructor invoked");
        setGisjoin(gisJoin);
        initSparkSession(master, mongoUri, database, collection);
        addClusterDependencyJars();
    }

    public void setFeatures(String[] features) {
        this.features = features;
    }

    public void setGisjoin(String gisJoin) {
        this.gisJoin = gisJoin;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String[] getFeatures() {
        return features;
    }

    public String getGisJoin() {
        return gisJoin;
    }

    public String getLabel() {
        return label;
    }

    public Boolean getBootstrap() {
        return isBootstrap;
    }

    public void setBootstrap(Boolean bootstrap) {
        isBootstrap = bootstrap;
    }

    public Double getSubsamplingRate() {
        return subsamplingRate;
    }

    public void setSubsamplingRate(Double subsamplingRate) {
        this.subsamplingRate = subsamplingRate;
    }

    public Integer getNumTrees() {
        return numTrees;
    }

    public void setNumTrees(Integer numTrees) {
        this.numTrees = numTrees;
    }

    public String getFeatureSubsetStrategy() {
        return featureSubsetStrategy;
    }

    public void setFeatureSubsetStrategy(String featureSubsetStrategy) {
        this.featureSubsetStrategy = featureSubsetStrategy;
    }

    public String getImpurity() {
        return impurity;
    }

    public void setImpurity(String impurity) {
        this.impurity = impurity;
    }

    public Integer getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(Integer maxDepth) {
        this.maxDepth = maxDepth;
    }

    public Integer getMaxBins() {
        return maxBins;
    }

    public void setMaxBins(Integer maxBins) {
        this.maxBins = maxBins;
    }

    /**
     * Configures and builds a SparkSession and JavaSparkContext, then adds required dependency JARs to the cluster.
     * @param master URI of the Spark master. Format: spark://<hostname>:<port>
     * @param mongoUri URI of the Mongo database router. Format: mongodb://<hostname>:<port>
     * @param database Name of the Mongo database to use.
     * @param collection Name of the Mongo collection to import from above database.
     */
    private void initSparkSession(String master, String mongoUri, String database, String collection) {

        String appName = "SUSTAIN RandomForest Regression Model";
        log.info("Initializing SparkSession using:\n\tmaster={}\n\tappName={}\n\tspark.mongodb.input.uri={}" +
                "\n\tspark.mongodb.input.database={}\n\tspark.mongodb.input.collection={}",
                master, appName, mongoUri, database, collection);

        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName(appName)
                .config("spark.mongodb.input.uri", mongoUri)
                .config("spark.mongodb.input.database", database)
                .config("spark.mongodb.input.collection", collection)
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        addClusterDependencyJars();
    }

    /**
     * Adds required dependency jars to the Spark Context member.
     */
    private void addClusterDependencyJars() {
        String[] jarPaths = {
            "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
            "build/libs/spark-core_2.12-3.0.1.jar",
            "build/libs/spark-mllib_2.12-3.0.1.jar",
            "build/libs/spark-sql_2.12-3.0.1.jar",
            "build/libs/bson-4.0.5.jar",
            "build/libs/mongo-java-driver-3.12.5.jar",
            //"build/libs/mongodb-driver-core-4.0.5.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }
    }

    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add(queryField);
        Collections.addAll(cols, this.features);
        cols.add(this.label);
        return convertListToSeq(cols);
    }

    /**
     * Converts a Java List<String> of inputs to a Scala Seq<String>
     * @param inputList The Java List<String> we wish to transform
     * @return A Scala Seq<String> representing the original input list
     */
    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    // CREATES SPARK CONTEXT USING INPUT PARAMETERS AND EXECUTES THE TRAINING
    public void buildAndRunModel() {
        log.info("Running Model...");

        ReadConfig readConfig = ReadConfig.create(sparkContext);

        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = collection.select("_id", desiredColumns());

        /*Dataset<Row> gisDataset = selected.filter(selected.col("gis_join").$eq$eq$eq(this.gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"*/

        Dataset<Row> gisDataset = selected.filter(selected.col(queryField).equalTo(gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"

        //System.out.println("++++++GGGGGGGGGGGGGGGGGGG++++++++"+Arrays.toString(gisDataset.columns())+" "+gisDataset.count());

        log.info("DATA TYPES: \n"+Arrays.toString(gisDataset.columns())+" "+gisDataset.dtypes());
        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(this.features)
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);
        //mergedDataset.show(5);

        //System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++TYPES:"+Arrays.toString(mergedDataset.columns())+Arrays.toString(mergedDataset.dtypes()));

        Dataset<Row>[] rds = mergedDataset.randomSplit(new double[]{trainSplit , 1.0d - trainSplit});
        Dataset<Row> trainrdd = rds[0];
        Dataset<Row> testrdd = rds[1];

        RandomForestRegressor rf = new RandomForestRegressor().setFeaturesCol("features").setLabelCol("label");

        // POPULATING USER PARAMETERS
        ingestParameters(rf);

        RandomForestRegressionModel rmodel = rf.fit(trainrdd);

        log.info("========================================TRAINING COMPLETE========================================");


        Dataset<Row> predictions = rmodel.transform(testrdd);

        log.info("========================================TESTING COMPLETE========================================");

        //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n"+Arrays.toString(predictions.columns())+"\n"+predictions.first());
        RegressionEvaluator eval = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName(errorType);

        this.rmse = eval.evaluate(predictions);

        eval.setMetricName("r2");

        this.r2 = eval.evaluate(predictions);
        log.info("EVALUATIONS: RMSE, R2: "+rmse+" "+r2);
        logModelResults();
        sparkContext.close();
    }

    // INJECTING USER-DEFINED PARAMETERS INTO MODEL
    private void ingestParameters(RandomForestRegressor rf) {
        if (this.isBootstrap != null) {
            rf.setBootstrap(isBootstrap);
        }
        if (this.subsamplingRate != null) {
            rf.setSubsamplingRate(subsamplingRate);
        }
        if (this.numTrees != null) {
            rf.setNumTrees(numTrees);
        }
        if (this.featureSubsetStrategy != null) {
            rf.setFeatureSubsetStrategy(featureSubsetStrategy);
        }
        if (this.impurity != null) {
            rf.setImpurity(impurity);
        }
        if (this.maxDepth != null) {
            rf.setMaxDepth(maxDepth);
        }
        if (this.maxBins != null) {
            rf.setMaxBins(maxBins);
        }

    }

    public void populateTest() {
        this.numTrees = 1;
    }

    private void logModelResults() {
        log.info("Results for GISJoin {}\n" +
                        "RMSE: {}\n" +
                        "R2: {}\n"
                        ,
                this.gisJoin, this.rmse, this.r2);
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {
        String[] features = {"max_eastward_wind","max_min_air_temperature"};
        String label = "min_eastward_wind";
        String gisJoins = "G0100290";
        String collection_name = "macav2";

        RFRegressionModel lrModel = new RFRegressionModel("spark://lattice-1.cs.colostate.edu:32531",
                "mongodb://lattice-46:27017", "sustaindb", collection_name, gisJoins);

        lrModel.populateTest();
        lrModel.setFeatures(features);
        lrModel.setLabel(label);
        lrModel.setGisjoin(gisJoins);

        lrModel.buildAndRunModel();
    }

}
