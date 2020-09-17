package org.sustain.census;

import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CensusFeature;
import org.sustain.CensusResolution;
import org.sustain.Predicate;
import org.sustain.SpatialOp;
import org.sustain.TargetedCensusRequest;
import org.sustain.TargetedCensusResponse;
import org.sustain.UnsupportedFeatureException;
import org.sustain.census.controller.PopulationController;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TargetedCensusQueryHandler {
    public static final Logger log = LogManager.getLogger(TargetedCensusQueryHandler.class);

    private final TargetedCensusRequest request;
    private final StreamObserver<TargetedCensusResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public TargetedCensusQueryHandler(TargetedCensusRequest request,
                                      StreamObserver<TargetedCensusResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleTargetedCensusQuery() {
        Predicate predicate = request.getPredicate();
        CensusFeature censusFeature = predicate.getCensusFeature();
        CensusResolution censusResolution = request.getResolution();
        SpatialOp spatialOp = request.getSpatialOp();
        String requestGeoJson = request.getRequestGeoJson();

        log.info("CensusFeature: " + censusFeature.toString());
        log.info("CensusResolution: " + censusResolution.toString());
        log.info("SpatialOp: " + spatialOp.toString() + "\n");
        String resolution = Constants.TARGET_RESOLUTIONS.get(censusResolution);

        HashMap<String, String> geoJsonMap = SpatialQueryUtil.getGeoList(requestGeoJson, resolution, spatialOp);
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        new TargetedCensusQueryHandler.StreamWriter(responseObserver, queue, geoJsonMap).start();

        switch (censusFeature) {
            case TotalPopulation:
                PopulationController.fetchTargetedTotalPopulation(request, new ArrayList<>(geoJsonMap.keySet()), queue);
                fetchingCompleted = true;
                break;
            case PopulationByAge:
                log.warn("Unsupported");
                responseObserver.onError(new UnsupportedFeatureException(Constants.CensusFeatures.POPULATION_BY_AGE));
                //PopulationController.fetchTargetedPopulationByAge(request, new ArrayList<>(geoJsonMap.keySet()),
                // queue);
                break;
            case MedianHouseholdIncome:
                log.warn("Unsupported");
                responseObserver.onError(new UnsupportedFeatureException(Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME));
                break;
            case Poverty:
                log.warn("Unsupported");
                responseObserver.onError(new UnsupportedFeatureException(Constants.CensusFeatures.POVERTY));
                break;
            case Race:
                responseObserver.onError(new UnsupportedFeatureException(Constants.CensusFeatures.RACE));
                break;
            case UNRECOGNIZED:
                responseObserver.onError(new Exception("Invalid census feature: " + censusFeature.toString()));
                responseObserver.onCompleted();
                log.warn("Invalid Census Feature requested");
        }
    }

    public class StreamWriter extends Thread {
        private final StreamObserver<TargetedCensusResponse> responseObserver;
        private volatile LinkedBlockingQueue<String> data;
        private HashMap<String, String> geoJsonMap;

        public StreamWriter(StreamObserver<TargetedCensusResponse> responseObserver, LinkedBlockingQueue<String> data
                , HashMap<String, String> geoJsonMap) {
            this.responseObserver = responseObserver;
            this.data = data;
            this.geoJsonMap = geoJsonMap;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    writeDataToStream(datum);
                    count++;
                }
            }

            for (String datum : data) {
                writeDataToStream(datum);
                count++;
            }
            log.info("Streaming completed! No. of entries: " + count);
            responseObserver.onCompleted();
        }

        private void writeDataToStream(String datum) {
            String gisJoinInDataRecord = JsonParser.parseString(datum).getAsJsonObject().get(
                    Constants.GIS_JOIN).toString().replace("\"", "");
            String responseGeoJson = geoJsonMap.get(gisJoinInDataRecord);
            responseObserver.onNext(TargetedCensusResponse.newBuilder()
                    .setData(datum)
                    .setResponseGeoJson(responseGeoJson)
                    .build());
        }
    }
}
