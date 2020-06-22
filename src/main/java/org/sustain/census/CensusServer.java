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
import org.sustain.census.controller.mysql.AgeController;
import org.sustain.census.controller.mysql.GeoIdResolver;
import org.sustain.census.controller.mysql.IncomeController;
import org.sustain.census.controller.mysql.PopulationController;
import org.sustain.census.controller.mysql.PovertyController;
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
            String requestGeoJson = request.getGeoJson();
            System.out.println(requestGeoJson);
            JsonObject inputGeoJson = JsonParser.parseString(requestGeoJson).getAsJsonObject();
            Geometry geometry = SpatialQueryUtil.constructPolygon(inputGeoJson);
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getCensusResolution());
            switch (censusFeature) {
                case TotalPopulation:
                    ArrayList<GeoJson> geoWithin = SpatialQueryUtil.findGeoWithin(resolution + "_geo", geometry);
                    List<SingleSpatialResponse> responseList = new ArrayList<>();
                    for (GeoJson geoJson : geoWithin) {
                        String populationResult =
                                org.sustain.census.controller.mongodb.PopulationController.getPopulationResults(resolution,
                                        geoJson.getProperties().getGisJoin());
                        SingleSpatialResponse response = SingleSpatialResponse.newBuilder()
                                .setData(populationResult)
                                .setGeoJson(geoJson.toJson())
                                .build();
                        responseList.add(response);
                    }
                    SpatialResponse populationSpatialResponse =
                            SpatialResponse.newBuilder().addAllSingleSpatialResponse(responseList).build();
                    responseObserver.onNext(populationSpatialResponse);
                    responseObserver.onCompleted();
                    break;
                case MedianHouseholdIncome:
                    log.warn("Not supported yet");
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

        @Override
        public void getTotalPopulation(TotalPopulationRequest request,
                                       StreamObserver<TotalPopulationResponse> responseObserver) {
            SpatialTemporalInfo spatialTemporalInfo = request.getSpatialTemporalInfo();
            String decade = Constants.DECADES.get(spatialTemporalInfo.getDecade());
            String resolutionKey = spatialTemporalInfo.getResolution();


            if (!isRequestValid(resolutionKey)) {
                return;
            }

            SpatialTemporalInfo.SpatialInfoCase spatialInfoCase = spatialTemporalInfo.getSpatialInfoCase();
            try {
                switch (spatialInfoCase) {
                    case SINGLECOORDINATE:
                        double latitude = spatialTemporalInfo.getSingleCoordinate().getLatitude();
                        double longitude = spatialTemporalInfo.getSingleCoordinate().getLongitude();

                        String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                        log.info("Resolved GeoID (FIPS): " + resolutionValue);

                        responseObserver.onNext(PopulationController.fetchTotalPopulation(resolutionKey,
                                resolutionValue, decade));
                        responseObserver.onCompleted();
                        break;
                    case BOUNDINGBOX:
                        double x1 = spatialTemporalInfo.getBoundingBox().getX1();
                        double y1 = spatialTemporalInfo.getBoundingBox().getY1();
                        double x2 = spatialTemporalInfo.getBoundingBox().getX2();
                        double y2 = spatialTemporalInfo.getBoundingBox().getY2();

                        boolean isValid = isBoundingBoxValid(x1, x2, y1, y2);
                        if (!isValid) {
                            return;
                        }

                        ArrayList<String> geoIds = GeoIdResolver.getGeoIdsInBoundingBox(x1, x2, y1, y2, resolutionKey);
                        if (geoIds.size() == 0) {
                            log.warn("No GeoIDs found for the entered bounding-box coordinates");
                            return;
                        }
                        responseObserver.onNext(PopulationController.getAveragedPopulation(resolutionKey, geoIds,
                                decade));
                        responseObserver.onCompleted();
                        break;
                    case SPATIALINFO_NOT_SET:
                        log.warn("SpatialInfo not set");
                }
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
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

        @Override
        public void getMedianAge(MedianAgeRequest request, StreamObserver<MedianAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLongitude();

            if (!isRequestValid(resolutionKey)) {
                return;
            }

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(AgeController.fetchMedianAge(resolutionKey, resolutionValue));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getMedianHouseholdIncome(MedianHouseholdIncomeRequest request,
                                             StreamObserver<MedianHouseholdIncomeResponse> responseObserver) {
            SpatialTemporalInfo spatialTemporalInfo = request.getSpatialTemporalInfo();
            String decade = Constants.DECADES.get(spatialTemporalInfo.getDecade());
            String resolutionKey = spatialTemporalInfo.getResolution();

            if (!isRequestValid(resolutionKey)) {
                return;
            }

            // to determine the spatialInfo type (single-coordinate/bounding-box)
            SpatialTemporalInfo.SpatialInfoCase spatialInfoCase = spatialTemporalInfo.getSpatialInfoCase();
            try {
                switch (spatialInfoCase) {
                    case SINGLECOORDINATE:
                        double latitude = spatialTemporalInfo.getSingleCoordinate().getLatitude();
                        double longitude = spatialTemporalInfo.getSingleCoordinate().getLongitude();

                        String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                        log.info("Resolved GeoID (FIPS): " + resolutionValue);

                        responseObserver.onNext(IncomeController.fetchMedianHouseholdIncome(resolutionKey,
                                resolutionValue, decade));
                        responseObserver.onCompleted();
                        break;
                    case BOUNDINGBOX:
                        double x1 = spatialTemporalInfo.getBoundingBox().getX1();
                        double y1 = spatialTemporalInfo.getBoundingBox().getY1();
                        double x2 = spatialTemporalInfo.getBoundingBox().getX2();
                        double y2 = spatialTemporalInfo.getBoundingBox().getY2();

                        boolean isValid = isBoundingBoxValid(x1, x2, y1, y2);
                        if (!isValid) {
                            return;
                        }

                        ArrayList<String> geoIds = GeoIdResolver.getGeoIdsInBoundingBox(x1, x2, y1, y2, resolutionKey);
                        if (geoIds.size() == 0) {
                            log.warn("No GeoIDs found for the entered bounding-box coordinates");
                            return;
                        }
                        responseObserver.onNext(IncomeController.getAveragedMedianHouseholdIncome(resolutionKey,
                                geoIds, decade));
                        responseObserver.onCompleted();
                        break;
                    case SPATIALINFO_NOT_SET:
                        log.warn("SpatialInfo not set");
                }
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        private boolean isBoundingBoxValid(double x1, double x2, double y1, double y2) {
            boolean valid = true;
            if (x1 >= x2) {
                log.warn("x2 must be greater than x1");
                valid = false;
            }
            if (y1 >= y2) {
                log.warn("y2 must be greater than y1");
                valid = false;
            }
            return valid;
        }

        @Override
        public void getPopulationByAge(PopulationByAgeRequest request,
                                       StreamObserver<PopulationByAgeResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLongitude();

            if (!isRequestValid(resolutionKey)) {
                return;
            }

            try {
                String resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PopulationController.fetchPopulationByAge(resolutionKey, resolutionValue));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
        }

        @Override
        public void getPoverty(PovertyRequest request, StreamObserver<PovertyResponse> responseObserver) {
            String resolutionKey = request.getSpatialTemporalInfo().getResolution();
            double latitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLatitude();
            double longitude = request.getSpatialTemporalInfo().getSingleCoordinate().getLongitude();

            if (!isRequestValid(resolutionKey)) {
                return;
            }

            String resolutionValue;
            try {
                resolutionValue = GeoIdResolver.resolveGeoId(latitude, longitude, resolutionKey);
                log.info("Resolved GeoID (FIPS): " + resolutionValue);

                responseObserver.onNext(PovertyController.fetchPovertyData(resolutionKey, resolutionValue));
                responseObserver.onCompleted();
            } catch (SQLException e) {
                log.error(e);
                e.printStackTrace();
            }
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
