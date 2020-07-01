package org.sustain.census;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.model.geojson.Geometry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.controller.mongodb.SpatialQueryUtil;
import org.sustain.census.model.GeoJson;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.sustain.census.ServerHelper.executeTargetedIncomeQuery;
import static org.sustain.census.ServerHelper.executeTargetedPopulationQuery;
import static org.sustain.census.ServerHelper.executeTargetedRaceQuery;


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

    static class CensusServerImpl extends CensusGrpc.CensusImplBase {
        @Override
        public void spatialQuery(SpatialRequest request, StreamObserver<SpatialResponse> responseObserver) {
            CensusFeature censusFeature = request.getCensusFeature();
            String requestGeoJson = request.getRequestGeoJson();
            CensusResolution censusResolution = request.getCensusResolution();
            SpatialOp spatialOp = request.getSpatialOp();
            System.out.println("CensusFeature: " + censusFeature.toString());
            System.out.println("CensusResolution: " + censusResolution.toString());
            System.out.println("SpatialOp: " + spatialOp.toString());

            JsonObject inputGeoJson = JsonParser.parseString(requestGeoJson).getAsJsonObject();
            Geometry geometry = SpatialQueryUtil.constructPolygon(inputGeoJson);
            String resolution = Constants.TARGET_RESOLUTIONS.get(censusResolution);

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

            switch (censusFeature) {
                case TotalPopulation:
                    List<SingleSpatialResponse> populationResponseList = new ArrayList<>();
                    for (GeoJson geoJson : geoJsonList) {
                        String populationResult =
                                org.sustain.census.controller.mongodb.PopulationController.getPopulationResults(resolution,
                                        geoJson.getProperties().getGisJoin());
                        SingleSpatialResponse response = SingleSpatialResponse.newBuilder()
                                .setData(populationResult)
                                .setResponseGeoJson(geoJson.toJson())
                                .build();
                        populationResponseList.add(response);
                    }
                    SpatialResponse populationSpatialResponse =
                            SpatialResponse.newBuilder().addAllSingleSpatialResponse(populationResponseList).build();
                    responseObserver.onNext(populationSpatialResponse);
                    responseObserver.onCompleted();
                    break;
                case MedianHouseholdIncome:
                    List<SingleSpatialResponse> incomeResponseList = new ArrayList<>();
                    for (GeoJson geoJson : geoJsonList) {
                        String populationResult =
                                org.sustain.census.controller.mongodb.IncomeController.getMedianHouseholdIncome(resolution,
                                        geoJson.getProperties().getGisJoin());
                        SingleSpatialResponse response = SingleSpatialResponse.newBuilder()
                                .setData(populationResult)
                                .setResponseGeoJson(geoJson.toJson())
                                .build();
                        incomeResponseList.add(response);
                    }
                    SpatialResponse incomeSpatialResponse =
                            SpatialResponse.newBuilder().addAllSingleSpatialResponse(incomeResponseList).build();
                    responseObserver.onNext(incomeSpatialResponse);
                    responseObserver.onCompleted();
                    break;
                case PopulationByAge:
                    log.warn("Not supported yet");
                    break;
                case Poverty:
                    log.warn("Not supported yet");
                    break;
                case Race:
                    log.warn("Not supported yet");
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
         * Execute a TargetedQuery - return geographical areas that satisfy a given value range of a census feature
         * Example 1: Retrieve all states where (population >= 1,000,000)
         * Example 2: Retrieve all counties where (median household income < $50,000/year)
         */
        @Override
        public void executeTargetedQuery(TargetedQueryRequest request,
                                         StreamObserver<TargetedQueryResponse> responseObserver) {
            Predicate predicate = request.getPredicate();
            String comparisonOp = Constants.COMPARISON_OPS.get(predicate.getComparisonOp());
            double comparisonValue = predicate.getComparisonValue();
            Predicate.Feature feature = predicate.getFeature();
            Decade _decade = predicate.getDecade();
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getResolution());

            if (!isRequestValid(resolution)) {
                return;
            }

            String decade = Constants.DECADES.get(_decade);

            try {
                switch (feature) {
                    case Population:
                        executeTargetedPopulationQuery(responseObserver, comparisonOp, comparisonValue,
                                resolution, decade);
                        break;
                    case Income:
                        executeTargetedIncomeQuery(responseObserver, comparisonOp, comparisonValue,
                                resolution, decade);
                        break;
                    case Race:
                        executeTargetedRaceQuery(responseObserver, Constants.EMPTY_COMPARISON_FIELD, comparisonOp,
                                comparisonValue,
                                resolution, decade);
                        break;
                    case UNRECOGNIZED:
                        log.warn("Invalid Census feature requested");
                        break;
                }
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }
    }
}
