package org.sustain.census;

import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.controller.IncomeController;
import org.sustain.census.controller.PopulationController;
import org.sustain.census.controller.RaceController;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class CensusQueryHandler {
    private static final Logger log = LogManager.getLogger(CensusQueryHandler.class);

    private final SpatialRequest request;
    private final StreamObserver<SpatialResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public CensusQueryHandler(SpatialRequest request, StreamObserver<SpatialResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleCensusQuery() {
        CensusFeature censusFeature = request.getCensusFeature();
        String requestGeoJson = request.getRequestGeoJson();
        CensusResolution censusResolution = request.getCensusResolution();
        SpatialOp spatialOp = request.getSpatialOp();
        System.out.println();
        log.info("CensusFeature: " + censusFeature.toString());
        log.info("CensusResolution: " + censusResolution.toString());
        log.info("SpatialOp: " + spatialOp.toString() + "\n");
        String resolution = Constants.TARGET_RESOLUTIONS.get(censusResolution);

        HashMap<String, String> geoJsonMap = SpatialQueryUtil.getGeoList(requestGeoJson, resolution, spatialOp);
        LinkedBlockingQueue<SpatialResponse> queue = new LinkedBlockingQueue<>();

        int recordsCount = 0;
        switch (censusFeature) {
            case TotalPopulation:
                ArrayList<String> totalPopulationResults =
                        PopulationController.getTotalPopulationResults(resolution,
                                new ArrayList<>(geoJsonMap.keySet()));
                for (String populationResult : totalPopulationResults) {
                    String gisJoinInDataRecord = JsonParser.parseString(populationResult).getAsJsonObject().get(
                            Constants.GIS_JOIN).toString().replace("\"", "");
                    String responseGeoJson = geoJsonMap.get(gisJoinInDataRecord);
                    recordsCount++;
                    SpatialResponse response = SpatialResponse.newBuilder()
                            .setData(populationResult)
                            .setResponseGeoJson(responseGeoJson)
                            .build();
                    responseObserver.onNext(response);
                }
                log.info(Constants.CensusFeatures.TOTAL_POPULATION + ": Streaming completed! No. of entries: " + recordsCount + "\n");
                responseObserver.onCompleted();
                break;
            case PopulationByAge:
                ArrayList<String> populationByAgeResults =
                        PopulationController.getPopulationByAgeResults(resolution,
                                new ArrayList<>(geoJsonMap.keySet()));
                for (String populationResult : populationByAgeResults) {
                    String gisJoinInDataRecord = JsonParser.parseString(populationResult).getAsJsonObject().get(
                            Constants.GIS_JOIN).toString().replace("\"", "");
                    String responseGeoJson = geoJsonMap.get(gisJoinInDataRecord);
                    recordsCount++;
                    SpatialResponse response = SpatialResponse.newBuilder()
                            .setData(populationResult)
                            .setResponseGeoJson(responseGeoJson)
                            .build();
                    responseObserver.onNext(response);
                }
                log.info(Constants.CensusFeatures.POPULATION_BY_AGE + ": Streaming completed! No. of " +
                        "entries: " + recordsCount + "\n");
                responseObserver.onCompleted();
                break;
            case MedianHouseholdIncome:
                for (String gisJoin : geoJsonMap.keySet()) {
                    String incomeResults =
                            IncomeController.getMedianHouseholdIncome(resolution,
                                    gisJoin);
                    if (incomeResults != null) {
                        recordsCount++;
                        SpatialResponse response = SpatialResponse.newBuilder()
                                .setData(incomeResults)
                                .setResponseGeoJson(geoJsonMap.get(gisJoin))
                                .build();
                        responseObserver.onNext(response);
                    }
                }
                log.info(Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME + ": Streaming completed! No. of " +
                        "entries: " + recordsCount + "\n");
                responseObserver.onCompleted();
                break;

            case Poverty:
                log.warn("Not supported yet");
                break;
            case Race:
                for (String gisJoin : geoJsonMap.keySet()) {
                    String raceResult =
                            RaceController.getRace(resolution,
                                    gisJoin);
                    if (raceResult != null) {
                        recordsCount++;
                        SpatialResponse response = SpatialResponse.newBuilder()
                                .setData(raceResult)
                                .setResponseGeoJson(geoJsonMap.get(gisJoin))
                                .build();
                        responseObserver.onNext(response);
                    }
                }
                log.info(Constants.CensusFeatures.RACE + ": Streaming completed! No. of " + "entries: " + recordsCount + "\n");
                responseObserver.onCompleted();
                break;
            case UNRECOGNIZED:
                responseObserver.onError(new Exception("Invalid census feature" + censusFeature.toString()));
                responseObserver.onCompleted();
                log.warn("Invalid Census Feature requested");
        }
    }

    private class StreamWriter extends Thread {
        private volatile LinkedBlockingQueue<String> data;
        private StreamObserver<SpatialResponse> responseObserver;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<SpatialResponse> responseObserver) {
            this.data = data;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    responseObserver.onNext(SpatialResponse.newBuilder()
                            .setData(datum)
                            .setResponseGeoJson("")
                            .build());
                }
            }

            for (String datum : data) {

            }
        }
    }
}
