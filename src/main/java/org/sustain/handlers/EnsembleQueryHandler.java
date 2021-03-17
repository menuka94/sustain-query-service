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

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.sustain.*;
import org.sustain.modeling.GBoostRegressionModel;
import org.sustain.modeling.RFRegressionModel;
import org.sustain.util.Constants;

public class EnsembleQueryHandler extends ModelHandler {

    private static final Logger log = LogManager.getLogger(EnsembleQueryHandler.class);

    public EnsembleQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver,
                                JavaSparkContext sparkContext) {
        super(request, responseObserver, sparkContext);
    }

    @Override
    boolean isValid(ModelRequest modelRequest) {
        // TODO: Saptashwa: Implement
        return false;
    }

    private RForestRegressionResponse buildRFModel(ModelRequest modelRequest, String gisJoin) {
        String sparkMaster = Constants.Spark.MASTER;
        String mongoUri = String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT);
        String dbName = Constants.DB.NAME;

        Collection collection = modelRequest.getCollections(0); // We only support 1 collection currently
        RFRegressionModel model = new RFRegressionModel(sparkMaster, mongoUri, dbName, collection.getName(),
                gisJoin);

        // Set parameters of Random Forest Regression Model
        RForestRegressionRequest rfRequest = modelRequest.getRForestRegressionRequest();

        int featuresCount = collection.getFeaturesCount();
        String[] features = new String[featuresCount];
        for (int i = 0; i < featuresCount; i++) {
            features[i] = collection.getFeatures(i);
        }

        model.setFeatures(features);
        model.setLabel(collection.getLabel());
        model.setBootstrap(rfRequest.getIsBootstrap());

        // CHECKING FOR VALID MODEL PARAMETER VALUES
        if (rfRequest.getSubsamplingRate() > 0 && rfRequest.getSubsamplingRate() <= 1)
            model.setSubsamplingRate(rfRequest.getSubsamplingRate());
        if (rfRequest.getNumTrees() > 0)
            model.setNumTrees(rfRequest.getNumTrees());
        if (rfRequest.getFeatureSubsetStrategy() != null && !rfRequest.getFeatureSubsetStrategy().isEmpty())
            model.setFeatureSubsetStrategy(rfRequest.getFeatureSubsetStrategy());
        if (rfRequest.getImpurity() != null && !rfRequest.getImpurity().isEmpty())
            model.setImpurity(rfRequest.getImpurity());
        if (rfRequest.getMaxDepth() > 0)
            model.setMaxDepth(rfRequest.getMaxDepth());
        if (rfRequest.getMaxBins() > 0)
            model.setMaxBins(rfRequest.getMaxBins());
        if (rfRequest.getTrainSplit() > 0 && rfRequest.getTrainSplit() < 1)
            model.setTrainSplit(rfRequest.getTrainSplit());
        if (rfRequest.getMinInfoGain() > 0)
            model.setMinInfoGain(rfRequest.getMinInfoGain());
        if (rfRequest.getMinInstancesPerNode() >= 1)
            model.setMinInstancesPerNode(rfRequest.getMinInstancesPerNode());
        if (rfRequest.getMinWeightFractionPerNode() >= 0.0 && rfRequest.getMinWeightFractionPerNode() < 0.5)
            model.setMinWeightFractionPerNode(rfRequest.getMinWeightFractionPerNode());

        model.buildAndRunModel();

        return RForestRegressionResponse.newBuilder()
                .setGisJoin(model.getGisJoin())
                .setRmse(model.getRmse())
                .setR2(model.getR2())
                .build();
    }


    private GBoostRegressionResponse buildGBModel(ModelRequest modelRequest, String gisJoin) {
        String sparkMaster = Constants.Spark.MASTER;
        String mongoUri = String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT);
        String dbName = Constants.DB.NAME;

        Collection collection = modelRequest.getCollections(0); // We only support 1 collection currently
        GBoostRegressionModel model = new GBoostRegressionModel(sparkMaster, mongoUri, dbName, collection.getName(),
                gisJoin);

        // Set parameters of Random Forest Regression Model
        GBoostRegressionRequest gbRequest = modelRequest.getGBoostRegressionRequest();

        int featuresCount = collection.getFeaturesCount();
        String[] features = new String[featuresCount];
        for (int i = 0; i < featuresCount; i++) {
            features[i] = collection.getFeatures(i);
        }

        model.setFeatures(features);
        model.setLabel(collection.getLabel());

        if (gbRequest.getLossType() != null && !gbRequest.getLossType().isEmpty())
            model.setLossType(gbRequest.getLossType());
        if (gbRequest.getMaxIter() > 0)
            model.setMaxIter(gbRequest.getMaxIter());
        if (gbRequest.getSubsamplingRate() > 0 && gbRequest.getSubsamplingRate() <= 1)
            model.setSubsamplingRate(gbRequest.getSubsamplingRate());
        if (gbRequest.getSubsamplingRate() > 0 && gbRequest.getSubsamplingRate() <= 1)
            model.setStepSize(gbRequest.getStepSize());
        if (gbRequest.getFeatureSubsetStrategy() != null && !gbRequest.getFeatureSubsetStrategy().isEmpty())
            model.setFeatureSubsetStrategy(gbRequest.getFeatureSubsetStrategy());
        if (gbRequest.getImpurity() != null && !gbRequest.getImpurity().isEmpty())
            model.setImpurity(gbRequest.getImpurity());
        if (gbRequest.getMaxDepth() > 0)
            model.setMaxDepth(gbRequest.getMaxDepth());
        if (gbRequest.getMaxBins() > 0)
            model.setMaxBins(gbRequest.getMaxBins());
        if (gbRequest.getTrainSplit() > 0)
            model.setTrainSplit(gbRequest.getTrainSplit());
        if (gbRequest.getMinInfoGain() > 0)
            model.setMinInfoGain(gbRequest.getMinInfoGain());
        if (gbRequest.getMinInstancesPerNode() >= 1)
            model.setMinInstancesPerNode(gbRequest.getMinInstancesPerNode());
        if (gbRequest.getMinWeightFractionPerNode() >= 0.0 && gbRequest.getMinWeightFractionPerNode() < 0.5)
            model.setMinWeightFractionPerNode(gbRequest.getMinWeightFractionPerNode());

        model.buildAndRunModel();

        return GBoostRegressionResponse.newBuilder()
                .setGisJoin(model.getGisJoin())
                .setRmse(model.getRmse())
                .setR2(model.getR2())
                .build();
    }

    /**
     * Checks the validity of a ModelRequest object, in the context of a Random Forest Regression request.
     * @param modelRequest The ModelRequest object populated by the gRPC endpoint.
     * @return Boolean true if the model request is valid, false otherwise.
     */
    private boolean isValidModelRequest(ModelRequest modelRequest) {
        if (modelRequest.getType().equals(ModelType.R_FOREST_REGRESSION) || modelRequest.getType().equals(ModelType.G_BOOST_REGRESSION)) {
            if (modelRequest.getCollectionsCount() == 1) {
                if (modelRequest.getCollections(0).getFeaturesCount() > 0) {
                    return (modelRequest.hasRForestRegressionRequest() || modelRequest.hasGBoostRegressionRequest());
                }
            }
        }

        return false;
    }

    @Override
    public void handleRequest() {
        if (isValidModelRequest(this.request)) {
            if (request.getType().equals(ModelType.R_FOREST_REGRESSION)) {
                RForestRegressionRequest req = this.request.getRForestRegressionRequest();
                for (String gisJoin : req.getGisJoinsList()) {
                    RForestRegressionResponse modelResults = buildRFModel(this.request, gisJoin);
                    ModelResponse response = ModelResponse.newBuilder()
                            .setRForestRegressionResponse(modelResults)
                            .build();

                    this.responseObserver.onNext(response);
                }
            } else if(request.getType().equals(ModelType.G_BOOST_REGRESSION)) {
                GBoostRegressionRequest req = this.request.getGBoostRegressionRequest();
                for (String gisJoin : req.getGisJoinsList()) {
                    GBoostRegressionResponse modelResults = buildGBModel(this.request, gisJoin);
                    ModelResponse response = ModelResponse.newBuilder()
                            .setGBoostRegressionResponse(modelResults)
                            .build();

                    this.responseObserver.onNext(response);
                }
            }
        } else {
            log.warn("Invalid Model Request!");
        }
    }
}

