/* ========================================================
 * EnsembleQueryHandler.java
 *   Captures input parameters into out regression model object
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
package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.*;
import org.sustain.SparkTask;
import org.sustain.modeling.GBoostRegressionModel;
import org.sustain.modeling.RFRegressionModel;
import org.sustain.util.Constants;
import org.sustain.SparkManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class EnsembleQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> {

    private static final Logger log = LogManager.getLogger(EnsembleQueryHandler.class);

    public EnsembleQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    protected class RFRegressionTask implements SparkTask<List<ModelResponse>> {

        private final RForestRegressionRequest rfRequest;
        private final Collection requestCollection;
        private final List<String> gisJoins;

        RFRegressionTask(ModelRequest modelRequest, List<String> gisJoins) {
            this.rfRequest = modelRequest.getRForestRegressionRequest();
            this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
            this.gisJoins = gisJoins;
        }

        @Override
        public List<ModelResponse> execute(JavaSparkContext sparkContext) throws Exception {

            // Create a custom Mongo-Spark ReadConfig
            Map<String, String> readOverrides = new HashMap<String, String>();
            String mongoUri = String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT);
            readOverrides.put("spark.mongodb.input.collection", requestCollection.getName());
            readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
            readOverrides.put("spark.mongodb.input.uri", mongoUri);
            ReadConfig readConfig = ReadConfig.create(sparkContext.getConf(), readOverrides);

            // Lazy-load the collection in as a DF
            Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();
            List<ModelResponse> modelResponses = new ArrayList<>();

            for (String gisJoin : this.gisJoins) {

                RFRegressionModel model = new RFRegressionModel.RFRegressionBuilder()
                        .forMongoCollection(mongoCollection)
                        .forGISJoin(gisJoin)
                        .forFeatures(requestCollection.getFeaturesList())
                        .forLabel(requestCollection.getLabel())
                        .withImpurity(rfRequest.getImpurity())
                        .withFeatureSubsetStrategy(rfRequest.getFeatureSubsetStrategy())
                        .withMinInstancesPerNode(rfRequest.getMinInstancesPerNode())
                        .withNumTrees(rfRequest.getNumTrees())
                        .withMaxDepth(rfRequest.getMaxDepth())
                        .withMaxBins(rfRequest.getMaxBins())
                        .withMinInfoGain(rfRequest.getMinInfoGain())
                        .withMinWeightFractionPerNode(rfRequest.getMinWeightFractionPerNode())
                        .withSubsamplingRate(rfRequest.getSubsamplingRate())
                        .withTrainSplit(rfRequest.getTrainSplit())
                        .withIsBootstrap(rfRequest.getIsBootstrap())
                        .build();

                // Submit task to Spark Manager
                boolean ok = model.train();
                if (ok) {
                    RForestRegressionResponse rsp = RForestRegressionResponse.newBuilder()
                            .setGisJoin(model.getGisJoin())
                            .setRmse(model.getRmse())
                            .setR2(model.getR2())
                            .build();

                    modelResponses.add(ModelResponse.newBuilder()
                            .setRForestRegressionResponse(rsp)
                            .build());
                } else {
                    log.info("Ran into a problem building a model for GISJoin {}, skipping.", gisJoin);
                }
            }
            return modelResponses;
        }
    }


    protected class GBRegressionTask implements SparkTask<List<ModelResponse>> {

        private final GBoostRegressionRequest gbRequest;
        private final Collection requestCollection;
        private final List<String> gisJoins;

        GBRegressionTask(ModelRequest modelRequest, List<String> gisJoins) {
            this.gbRequest = modelRequest.getGBoostRegressionRequest();
            this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
            this.gisJoins = gisJoins;
        }

        @Override
        public List<ModelResponse> execute(JavaSparkContext sparkContext) throws Exception {

            // Create a custom Mongo-Spark ReadConfig
            Map<String, String> readOverrides = new HashMap<String, String>();
            String mongoUri = String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT);
            readOverrides.put("spark.mongodb.input.collection", requestCollection.getName());
            readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
            readOverrides.put("spark.mongodb.input.uri", mongoUri);
            ReadConfig readConfig = ReadConfig.create(sparkContext.getConf(), readOverrides);

            // Lazy-load the collection in as a DF
            Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();
            List<ModelResponse> modelResponses = new ArrayList<>();

            for (String gisJoin : this.gisJoins) {

                GBoostRegressionModel model = new GBoostRegressionModel.GradientBoostRegressionBuilder()
                        .forMongoCollection(mongoCollection)
                        .forGISJoin(gisJoin)
                        .forFeatures(requestCollection.getFeaturesList())
                        .forLabel(requestCollection.getLabel())
                        .withLossType(gbRequest.getLossType())
                        .withImpurity(gbRequest.getImpurity())
                        .withFeatureSubsetStrategy(gbRequest.getFeatureSubsetStrategy())
                        .withMinInstancesPerNode(gbRequest.getMinInstancesPerNode())
                        .withMaxDepth(gbRequest.getMaxDepth())
                        .withMaxIterations(gbRequest.getMaxIter())
                        .withMaxBins(gbRequest.getMaxBins())
                        .withMinInfoGain(gbRequest.getMinInfoGain())
                        .withMinWeightFractionPerNode(gbRequest.getMinWeightFractionPerNode())
                        .withSubsamplingRate(gbRequest.getSubsamplingRate())
                        .withStepSize(gbRequest.getStepSize())
                        .withTrainSplit(gbRequest.getTrainSplit())
                        .build();

                // Submit task to Spark Manager
                boolean ok = model.train();
                if (ok) {
                    RForestRegressionResponse rsp = RForestRegressionResponse.newBuilder()
                            .setGisJoin(model.getGisJoin())
                            .setRmse(model.getRmse())
                            .setR2(model.getR2())
                            .build();

                    modelResponses.add(ModelResponse.newBuilder()
                            .setRForestRegressionResponse(rsp)
                            .build());
                } else {
                    log.info("Ran into a problem building a model for GISJoin {}, skipping.", gisJoin);
                }
            }
            return modelResponses;
        }
    }

    /**
     * Checks the validity of a ModelRequest object, in the context of a Random Forest Regression request.
     * @param modelRequest The ModelRequest object populated by the gRPC endpoint.
     * @return Boolean true if the model request is valid, false otherwise.
     */
    @Override
    public boolean isValid(ModelRequest modelRequest) {
        if (modelRequest.getType().equals(ModelType.R_FOREST_REGRESSION) || modelRequest.getType().equals(ModelType.G_BOOST_REGRESSION)) {
            if (modelRequest.getCollectionsCount() == 1) {
                if (modelRequest.getCollections(0).getFeaturesCount() > 0) {
                    return (modelRequest.hasRForestRegressionRequest() || modelRequest.hasGBoostRegressionRequest());
                }
            }
        }

        return false;
    }


    private List<List<String>> batchGisJoins(List<String> gisJoins, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        int totalGisJoins = gisJoins.size();
        int gisJoinsPerBatch = (int) Math.ceil( (1.0*totalGisJoins) / (1.0*batchSize) );
        log.info(">>> Max batch size: {}, totalGisJoins: {}, gisJoinsPerBatch: {}", batchSize, totalGisJoins,
                gisJoinsPerBatch);

        for (int i = 0; i < totalGisJoins; i++) {
            if ( i % gisJoinsPerBatch == 0 ) {
                batches.add(new ArrayList<>());
            }
            String gisJoin = gisJoins.get(i);
            batches.get(batches.size() - 1).add(gisJoin);
        }

        StringBuilder batchLog = new StringBuilder(
                String.format(">>> %d batches for %d GISJoins\n", batches.size(), totalGisJoins)
        );
        for (int i = 0; i < batches.size(); i++) {
            batchLog.append(String.format("\tBatch %d size: %d\n", i, batches.get(i).size()));
        }
        log.info(batchLog.toString());
        return batches;
    }

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {

            if (request.getType().equals(ModelType.R_FOREST_REGRESSION)) {
                try {
                    RForestRegressionRequest req = this.request.getRForestRegressionRequest();

                    List<List<String>> gisJoinBatches = batchGisJoins(req.getGisJoinsList(), 20);

                    List<Future<List<ModelResponse>>> batchedModelTasks = new ArrayList<>();
                    for (List<String> gisJoinBatch: gisJoinBatches) {
                        RFRegressionTask rfTask = new RFRegressionTask(this.request, gisJoinBatch);
                        batchedModelTasks.add(this.sparkManager.submit(rfTask, "rf-regression-query"));
                    }

                    // Wait for each task to complete and return their ModelResponses
                    for (Future<List<ModelResponse>> indvTask: batchedModelTasks) {
                        List<ModelResponse> batchedModelResponses = indvTask.get();
                        for (ModelResponse modelResponse: batchedModelResponses) {
                            this.responseObserver.onNext(modelResponse);
                        }
                    }

                } catch (Exception e) {
                    log.error("Failed to evaluate query", e);
                    responseObserver.onError(e);
                }
            } else if(request.getType().equals(ModelType.G_BOOST_REGRESSION)) {

                try {
                    GBoostRegressionRequest req = this.request.getGBoostRegressionRequest();

                    List<List<String>> gisJoinBatches = batchGisJoins(req.getGisJoinsList(), 20);

                    List<Future<List<ModelResponse>>> batchedModelTasks = new ArrayList<>();
                    for (List<String> gisJoinBatch: gisJoinBatches) {
                        GBRegressionTask gbTask = new GBRegressionTask(this.request, gisJoinBatch);
                        batchedModelTasks.add(this.sparkManager.submit(gbTask, "gb-regression-query"));
                    }

                    // Wait for each task to complete and return their ModelResponses
                    for (Future<List<ModelResponse>> indvTask: batchedModelTasks) {
                        List<ModelResponse> batchedModelResponses = indvTask.get();
                        for (ModelResponse modelResponse: batchedModelResponses) {
                            this.responseObserver.onNext(modelResponse);
                        }
                    }

                } catch (Exception e) {
                    log.error("Failed to evaluate query", e);
                    responseObserver.onError(e);
                }
            }
        } else {
            log.warn("Invalid Model Request!");
        }
    }
}
