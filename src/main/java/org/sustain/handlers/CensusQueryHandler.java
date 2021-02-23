package org.sustain.handlers;

import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CensusFeature;
import org.sustain.CensusRequest;
import org.sustain.CensusResolution;
import org.sustain.CensusResponse;
import org.sustain.SpatialOp;
import org.sustain.controllers.IncomeController;
import org.sustain.controllers.PopulationController;
import org.sustain.controllers.RaceController;
import org.sustain.controllers.SpatialQueryUtil;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class CensusQueryHandler {
    private static final Logger log = LogManager.getLogger(CensusQueryHandler.class);

    private final CensusRequest request;
    private final StreamObserver<CensusResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public CensusQueryHandler(CensusRequest request, StreamObserver<CensusResponse> responseObserver) {
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
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        new StreamWriter(queue, responseObserver, geoJsonMap).start();

        int recordsCount = 0;
        switch (censusFeature) {
            case TotalPopulation:
                PopulationController.getTotalPopulationResults(resolution, new ArrayList<>(geoJsonMap.keySet()), queue);
                fetchingCompleted = true;
                break;
            case PopulationByAge:
                PopulationController.getPopulationByAgeResults(resolution, new ArrayList<>(geoJsonMap.keySet()), queue);
                break;
            case MedianHouseholdIncome:
                for (String gisJoin : geoJsonMap.keySet()) {
                    String incomeResults =
                            IncomeController.getMedianHouseholdIncome(resolution,
                                    gisJoin);
                    if (incomeResults != null) {
                        recordsCount++;
                        CensusResponse response = CensusResponse.newBuilder()
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
                        CensusResponse response = CensusResponse.newBuilder()
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
                responseObserver.onError(new Exception("Invalid census feature: " + censusFeature.toString()));
                responseObserver.onCompleted();
                log.warn("Invalid Census Feature requested");
        }
    }

    private class StreamWriter extends Thread {
        private final StreamObserver<CensusResponse> responseObserver;
        private volatile LinkedBlockingQueue<String> data;
        private HashMap<String, String> geoJsonMap;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<CensusResponse> responseObserver,
                            HashMap<String, String> geoJsonMap) {
            this.data = data;
            this.responseObserver = responseObserver;
            this.geoJsonMap = geoJsonMap;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    writeDatumToStream(datum);
                    count++;
                }
            }

            // if there is any data remaining in the queue after fetching is completed
            for (String datum : data) {
                writeDatumToStream(datum);
                count++;
            }
            log.info("Streaming completed! No. of entries: " + count);
            responseObserver.onCompleted();
        }

        private void writeDatumToStream(String datum) {
            String gisJoinInDataRecord = JsonParser.parseString(datum).getAsJsonObject().get(
                    Constants.GIS_JOIN).toString().replace("\"", "");
            String responseGeoJson = geoJsonMap.get(gisJoinInDataRecord);
            responseObserver.onNext(CensusResponse.newBuilder()
                    .setData(datum)
                    .setResponseGeoJson(responseGeoJson)
                    .build());

        }
    }
}
