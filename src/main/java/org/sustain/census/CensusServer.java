package org.sustain.census;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.model.geojson.Geometry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.OsmQueryHandler;
import org.sustain.otherDatasets.controller.DatasetController;
import org.sustain.census.controller.PopulationController;
import org.sustain.census.controller.RaceController;
import org.sustain.census.controller.IncomeController;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.census.model.GeoJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class CensusServer {
    private static final Logger log = LogManager.getLogger(CensusServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CensusServer server = new CensusServer();
        server.start();
        server.blockUntilShutdown();
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
                    CensusServer.this.stop();
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

    static ArrayList<GeoJson> getGeoList(String requestGeoJson, String resolution, SpatialOp spatialOp) {
        JsonObject inputGeoJson = JsonParser.parseString(requestGeoJson).getAsJsonObject();
        Geometry geometry = SpatialQueryUtil.constructPolygon(inputGeoJson);

        String collectionName = resolution + "_geo";
        log.debug("collectionName: " + collectionName);
        ArrayList<GeoJson> geoJsonList = null;
        switch (spatialOp) {
            case GeoWithin:
                geoJsonList = SpatialQueryUtil.findGeoWithin(collectionName, geometry);
                break;
            case GeoIntersects:
                geoJsonList = SpatialQueryUtil.findGeoIntersects(collectionName, geometry);
                break;
            case UNRECOGNIZED:
                geoJsonList = new ArrayList<>();
                log.warn("Unrecognized Spatial Operation");
        }
        log.info("geoJsonList.size(): " + geoJsonList.size());
        return geoJsonList;
    }

    // Server implementation
    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void spatialQuery(SpatialRequest request, StreamObserver<SpatialResponse> responseObserver) {
            CensusFeature censusFeature = request.getCensusFeature();
            String requestGeoJson = request.getRequestGeoJson();
            CensusResolution censusResolution = request.getCensusResolution();
            SpatialOp spatialOp = request.getSpatialOp();
            log.info("CensusFeature: " + censusFeature.toString());
            log.info("CensusResolution: " + censusResolution.toString());
            log.info("SpatialOp: " + spatialOp.toString());
            String resolution = Constants.TARGET_RESOLUTIONS.get(censusResolution);

            ArrayList<GeoJson> geoJsonList = getGeoList(requestGeoJson, resolution, spatialOp);

            switch (censusFeature) {
                case TotalPopulation:
                    int i = 0;
                    for (GeoJson geoJson : geoJsonList) {
                        String populationResult =
                                PopulationController.getPopulationResults(resolution,
                                        geoJson.getProperties().getGisJoin());
                        if (populationResult != null) {
                            i++;
                            SpatialResponse response = SpatialResponse.newBuilder()
                                    .setData(populationResult)
                                    .setResponseGeoJson(geoJson.toJson())
                                    .build();
                            responseObserver.onNext(response);
                        }
                    }
                    log.info("Fetching completed! No. of entries: " + i + "\n");
                    responseObserver.onCompleted();
                    break;
                case MedianHouseholdIncome:
                    for (GeoJson geoJson : geoJsonList) {
                        String populationResult =
                                IncomeController.getMedianHouseholdIncome(resolution,
                                        geoJson.getProperties().getGisJoin());
                        SpatialResponse response = SpatialResponse.newBuilder()
                                .setData(populationResult)
                                .setResponseGeoJson(geoJson.toJson())
                                .build();
                        responseObserver.onNext(response);
                    }
                    responseObserver.onCompleted();
                    break;
                case PopulationByAge:
                    log.warn("Not supported yet");
                    break;
                case Poverty:
                    log.warn("Not supported yet");
                    break;
                case Race:
                    for (GeoJson geoJson : geoJsonList) {
                        String populationResult =
                                RaceController.getRace(resolution,
                                        geoJson.getProperties().getGisJoin());
                        SpatialResponse response = SpatialResponse.newBuilder()
                                .setData(populationResult)
                                .setResponseGeoJson(geoJson.toJson())
                                .build();
                        responseObserver.onNext(response);
                    }
                    responseObserver.onCompleted();
                    break;
                case UNRECOGNIZED:
                    log.warn("Unknown Census Feature requested");
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
        @Override
        public void executeTargetedCensusQuery(TargetedCensusRequest request,
                                               StreamObserver<TargetedCensusResponse> responseObserver) {
            Predicate predicate = request.getPredicate();
            double comparisonValue = predicate.getComparisonValue();
            Predicate.ComparisonOperator comparisonOp = predicate.getComparisonOp();
            CensusFeature censusFeature = predicate.getCensusFeature();
            Decade _decade = predicate.getDecade();
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getResolution());

            String decade = Constants.DECADES.get(_decade);
            ArrayList<GeoJson> geoList = getGeoList(request.getRequestGeoJson(), resolution, request.getSpatialOp());

            try {
                switch (censusFeature) {
                    case TotalPopulation:
                        String comparisonField = decade + "_" + Constants.CensusFeatures.TOTAL_POPULATION;
                        for (GeoJson geoJson : geoList) {
                            String populationResult =
                                    PopulationController.getPopulationResults(resolution,
                                            geoJson.getProperties().getGisJoin());
                            double value =
                                    JsonParser.parseString(populationResult).getAsJsonObject().get(comparisonField).getAsDouble();
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
        }


        @Override
        public void osmQuery(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
            OsmQueryHandler handler = new OsmQueryHandler(request, responseObserver);
            handler.handleOsmQuery();
        }

        @Override
        public void datasetQuery(DatasetRequest request, StreamObserver<DatasetResponse> responseObserver) {
            ArrayList<String> data = DatasetController.getData(request);
            for (String datum : data) {
                responseObserver.onNext(DatasetResponse.newBuilder().setResponse(datum).build());
            }
            responseObserver.onCompleted();
        }

    }   // end of Server implementation
}
