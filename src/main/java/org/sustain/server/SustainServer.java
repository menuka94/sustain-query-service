package org.sustain.server;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.model.geojson.Geometry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.CensusFeature;
import org.sustain.census.CensusGrpc;
import org.sustain.census.CensusResolution;
import org.sustain.census.DatasetRequest;
import org.sustain.census.DatasetResponse;
import org.sustain.census.OsmRequest;
import org.sustain.census.OsmResponse;
import org.sustain.census.Predicate;
import org.sustain.census.SpatialOp;
import org.sustain.census.SpatialRequest;
import org.sustain.census.SpatialResponse;
import org.sustain.census.controller.IncomeController;
import org.sustain.census.controller.PopulationController;
import org.sustain.census.controller.RaceController;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.openStreetMaps.OsmQueryHandler;
import org.sustain.otherDatasets.controller.DatasetController;
import org.sustain.util.Constants;
import org.sustain.util.model.GeoJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class SustainServer {
    private static final Logger log = LogManager.getLogger(SustainServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final SustainServer server = new SustainServer();
        server.start();
        server.blockUntilShutdown();
    }

    static HashMap<String, GeoJson> getGeoList(String requestGeoJson, String resolution, SpatialOp spatialOp) {
        JsonObject inputGeoJson = JsonParser.parseString(requestGeoJson).getAsJsonObject();
        Geometry geometry = SpatialQueryUtil.constructPolygon(inputGeoJson);
        log.info("Geometry constructed");

        String collectionName = resolution + "_geo";
        log.info("collectionName: " + collectionName);
        HashMap<String, GeoJson> geoJsonMap = null;
        switch (spatialOp) {
            case GeoWithin:
                log.info("case GeoWithin");
                geoJsonMap = SpatialQueryUtil.findGeoWithin(collectionName, geometry);
                break;
            case GeoIntersects:
                log.info("case GeoIntersects");
                geoJsonMap = SpatialQueryUtil.findGeoIntersects(collectionName, geometry);
                break;
            case UNRECOGNIZED:
                geoJsonMap = new HashMap<>();
                log.warn("Unrecognized Spatial Operation");
        }
        log.info("geoJsonMap.size(): " + geoJsonMap.size());
        return geoJsonMap;
    }

    public void start() throws IOException {
        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
                .addService(new CensusServerImpl())
                .build().start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    SustainServer.this.stop();
                } catch (InterruptedException e) {
                    log.error("Error in stopping the server");
                    e.printStackTrace();
                }
                log.warn("Server is shutting down");
            }
        });
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

    // Server implementation
    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void spatialQuery(SpatialRequest request, StreamObserver<SpatialResponse> responseObserver) {
            CensusFeature censusFeature = request.getCensusFeature();
            String requestGeoJson = request.getRequestGeoJson();
            CensusResolution censusResolution = request.getCensusResolution();
            SpatialOp spatialOp = request.getSpatialOp();
            System.out.println();
            log.info("CensusFeature: " + censusFeature.toString());
            log.info("CensusResolution: " + censusResolution.toString());
            log.info("SpatialOp: " + spatialOp.toString() + "\n");
            String resolution = Constants.TARGET_RESOLUTIONS.get(censusResolution);

            HashMap<String, GeoJson> geoJsonMap = getGeoList(requestGeoJson, resolution, spatialOp);

            int recordsCount = 0;
            switch (censusFeature) {
                case TotalPopulation:
                    ArrayList<String> totalPopulationResults =
                            PopulationController.getTotalPopulationResults(resolution,
                                    new ArrayList<>(geoJsonMap.keySet()));
                    for (String populationResult : totalPopulationResults) {
                        String gisJoinInDataRecord = JsonParser.parseString(populationResult).getAsJsonObject().get(
                                Constants.GIS_JOIN).toString().replace("\"", "");
                        String responseGeoJson =
                                geoJsonMap.get(gisJoinInDataRecord).toJson();
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
                        String responseGeoJson =
                                geoJsonMap.get(gisJoinInDataRecord).toJson();
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
                    for (GeoJson geoJson : geoJsonMap.values()) {
                        String incomeResults =
                                IncomeController.getMedianHouseholdIncome(resolution,
                                        geoJson.getProperties().getGisJoin());
                        if (incomeResults != null) {
                            recordsCount++;
                            SpatialResponse response = SpatialResponse.newBuilder()
                                    .setData(incomeResults)
                                    .setResponseGeoJson(geoJson.toJson())
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
                    for (GeoJson geoJson : geoJsonMap.values()) {
                        String raceResult =
                                RaceController.getRace(resolution,
                                        geoJson.getProperties().getGisJoin());
                        if (raceResult != null) {
                            recordsCount++;
                            SpatialResponse response = SpatialResponse.newBuilder()
                                    .setData(raceResult)
                                    .setResponseGeoJson(geoJson.toJson())
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

        private boolean isRequestValid(String resolution) {
            boolean valid = true;
            if ("".equals(resolution)) {
                log.warn("Resolution is empty.");
                valid = false;
            }

            return valid;
        }

        /**
         * Determine if a given value is valid after comparing it with the actual value using the\
         * given comparison Operator
         *
         * @param comparisonOp    comparison operator from gRPC request
         * @param value           value returned by the census data record
         * @param comparisonValue comparisonValue from gRPC request
         * @return comparison of 'value' to 'comparisonValue'
         */
        boolean compareValueWithInputValue(Predicate.ComparisonOperator comparisonOp, Double value,
                                           Double comparisonValue) {
            switch (comparisonOp) {
                case EQUAL:
                    return value.equals(comparisonValue);
                case GREATER_THAN_OR_EQUAL:
                    return value >= comparisonValue;
                case LESS_THAN:
                    return value < comparisonValue;
                case LESS_THAN_OR_EQUAL:
                    return value <= comparisonValue;
                case GREATER_THAN:
                    return value > comparisonValue;
                case UNRECOGNIZED:
                    log.warn("Unknown comparison operator");
                    return false;
            }
            return false;
        }

        /**
         * Execute a TargetedQuery - return geographical areas that satisfy a given value range of a census feature
         * Example 1: Retrieve all counties where (population >= 1,000,000)
         * Example 2: Retrieve all tracts where (median household income < $50,000/year)
         */
/*        @Override
        public void executeTargetedCensusQuery(TargetedCensusRequest request,
                                               StreamObserver<TargetedCensusResponse> responseObserver) {
            Predicate predicate = request.getPredicate();
            double comparisonValue = predicate.getComparisonValue();
            Predicate.ComparisonOperator comparisonOp = predicate.getComparisonOp();
            CensusFeature censusFeature = predicate.getCensusFeature();
            Decade _decade = predicate.getDecade();
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getResolution());

            String decade = Constants.DECADES.get(_decade);
            HashMap<String, GeoJson> geoJsonMap = getGeoList(request.getRequestGeoJson(), resolution,
                    request.getSpatialOp());

            try {
                switch (censusFeature) {
                    case TotalPopulation:
                        String comparisonField = decade + "_" + Constants.CensusFeatures.TOTAL_POPULATION;
                        for (GeoJson geoJson : geoList) {
                            String populationResult =
                                    PopulationController.getTotalPopulationResults(resolution,
                                            geoJson.getProperties().getGisJoin());
                            double value =
                                    JsonParser.parseString(populationResult).getAsJsonObject().get(comparisonField)
                                    .getAsDouble();
                            boolean valid = compareValueWithInputValue(comparisonOp, value, comparisonValue);

                            if (valid) {
                                SpatialResponse response = SpatialResponse.newBuilder()
                                        .setData(populationResult)
                                        .setResponseGeoJson(geoJson.toJson())
                                        .build();
                                responseObserver.onNext(TargetedCensusResponse.newBuilder().setSpatialResponse(response)
                                        .build());
                            }
                        }   // end of geoJson loop

                        responseObserver.onCompleted();
                        break;
                    case MedianHouseholdIncome:
                        HashMap<String, String> incomeResults = IncomeController.fetchTargetedInfo(decade,
                                resolution, comparisonOp, comparisonValue);
                        break;
                    case Race:
                        break;
                    case UNRECOGNIZED:
                        log.warn("Invalid Census censusFeature requested");
                        break;
                }   // end of census-feature switch
            } catch (Exception e) {
                log.error(e);
                e.printStackTrace();
            }
        }*/

        @Override
        public void osmQuery(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
            OsmQueryHandler handler = new OsmQueryHandler(request, responseObserver);
            handler.handleOsmQuery();
        }

        @Override
        public void datasetQuery(DatasetRequest request, StreamObserver<DatasetResponse> responseObserver) {
            ArrayList<String> data = DatasetController.getData(request);
            int count = 0;
            for (String datum : data) {
                responseObserver.onNext(DatasetResponse.newBuilder().setResponse(datum).build());
                count++;
            }
            log.info("Fetching completed! Count: " + count);
            responseObserver.onCompleted();
        }

    }   // end of Server implementation
}
