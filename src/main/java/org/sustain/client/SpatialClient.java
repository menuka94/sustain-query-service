package org.sustain.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.util.Constants;

import java.util.Iterator;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private SustainGrpc.SustainBlockingStub sustainBlockingStub;
    private JsonProxyGrpc.JsonProxyBlockingStub jsonProxyBlockingStub;

    public SpatialClient() {
        String target = Constants.Server.HOST + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
        jsonProxyBlockingStub = JsonProxyGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        logEnvironment();

        SpatialClient spatialClient = new SpatialClient();
        SustainGrpc.SustainBlockingStub sustainBlockingStub = spatialClient.getSustainBlockingStub();
        JsonProxyGrpc.JsonProxyBlockingStub jsonProxyBlockingStub = spatialClient.getJsonProxyBlockingStub();

        exampleLRModelRequest(jsonProxyBlockingStub);
        //exampleKMeansClusteringRequest(jsonProxyBlockingStub);
        //exampleSpatialQuery(sustainBlockingStub, geoJson);
        //exampleTargetedQuery(sustainBlockingStub, geoJson);
        //exampleOsmQuery(sustainBlockingStub, SampleGeoJson.FORT_COLLINS);
        //exampleDatasetQuery(DatasetRequest.Dataset.FIRE_STATIONS, sustainBlockingStub, SampleGeoJson.MULTIPLE_STATES);
        //exampleSviQuery(SampleGeoJson.COLORADO, SpatialOp.GeoIntersects, sustainBlockingStub);
    }

    // Logs the environment variables that the server was started with.
    public static void logEnvironment() {
        log.info("\n\n--- Server Environment ---\n" +
                        "SERVER_HOST: {}\n" +
                        "SERVER_PORT: {}\n" +
                        "\n\n--- Database Environment ---\n" +
                        "DB_HOST: {}\n" +
                        "DB_PORT: {}\n" +
                        "DB_NAME: {}\n" +
                        "DB_USERNAME: {}\n" +
                        "DB_PASSWORD: {}\n", Constants.Server.HOST, Constants.Server.PORT, Constants.DB.HOST,
                Constants.DB.PORT, Constants.DB.NAME, Constants.DB.USERNAME, Constants.DB.PASSWORD);
    }


    private static void exampleDatasetQuery(DatasetRequest.Dataset dataset,
                                            SustainGrpc.SustainBlockingStub sustainBlockingStub, String geoJson) {
        DatasetRequest request = DatasetRequest.newBuilder()
                .setDataset(dataset)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();
        Iterator<DatasetResponse> datasetResponseIterator = sustainBlockingStub.datasetQuery(request);
        int count = 0;
        while (datasetResponseIterator.hasNext()) {
            DatasetResponse response = datasetResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleSviQuery(String geoJson, SpatialOp spatialOp,
                                        SustainGrpc.SustainBlockingStub sustainBlockingStub) {
        SviRequest request = SviRequest.newBuilder()
                .setRequestGeoJson(geoJson)
                .setSpatialOp(spatialOp)
                .build();

        Iterator<SviResponse> responseIterator = sustainBlockingStub.sviQuery(request);
        int count = 0;
        while (responseIterator.hasNext()) {
            SviResponse response = responseIterator.next();
            count++;
            log.info(response.getData());
            //log.info(response.getResponseGeoJson());
            System.out.println();
        }
        log.info("Count: " + count);
    }

    private static void exampleOsmQuery(SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
        OsmRequest request = OsmRequest.newBuilder()
                .setDataset(OsmRequest.Dataset.ALL)
                .setSpatialOp(SpatialOp.GeoWithin)
                // .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                //         .setKey("properties.highway")
                //         .setValue("primary"))
                // .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                //         .setKey("properties.highway")
                //         .setValue("residential"))
                .setRequestGeoJson(geoJson).build();

        Iterator<OsmResponse> osmResponseIterator = censusBlockingStub.osmQuery(request);
        int count = 0;
        while (osmResponseIterator.hasNext()) {
            OsmResponse response = osmResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleLRModelRequest(JsonProxyGrpc.JsonProxyBlockingStub jsonProxyBlockingStub) {
        String request = "{\n" +
                "    \"type\": \"LINEAR_REGRESSION\",\n" +
                "    \"collections\": [\n" +
                "      {\n" +
                "        \"name\": \"macav2\",\n" +
                "        \"label\": \"max_max_air_temperature\",\n" +
                "        \"features\": [\n" +
                "          \"timestamp\"\n" +
                "        ]\n" +
                "      }\n" +
                "    ],\n" +
                "    \"linearRegressionRequest\": {\n" +
                "      \"gisJoins\": [\n" +
                "        \"G0100290\",\n" +
                "        \"G0100210\",\n" +
                "        \"G0100190\",\n" +
                "        \"G0100230\"\n" +
                "      ],\n" +
                "      \"loss\": \"squaredError\",\n" +
                "      \"solver\": \"auto\",\n" +
                "      \"aggregationDepth\": 2,\n" +
                "      \"maxIterations\": 10,\n" +
                "      \"elasticNetParam\": 0.0,\n" +
                "      \"epsilon\": 1.35,\n" +
                "      \"regularizationParam\": 0.5,\n" +
                "      \"convergenceTolerance\": 0.000001,\n" +
                "      \"fitIntercept\": true,\n" +
                "      \"setStandardization\": true\n" +
                "    }\n" +
                "}";

        log.info("Sending JSON request:\n{}", request);

        JsonModelRequest modelRequest = JsonModelRequest.newBuilder()
                .setJson(request)
                .build();

        Iterator<JsonModelResponse> jsonResponseIterator = jsonProxyBlockingStub.modelQuery(modelRequest);
        while (jsonResponseIterator.hasNext()) {
            JsonModelResponse jsonResponse = jsonResponseIterator.next();
            log.info("JSON Model Response: {}", jsonResponse.getJson());
        }

    }

    public static void exampleKMeansClusteringRequest(JsonProxyGrpc.JsonProxyBlockingStub jsonProxyBlockingStub) {
        String request = "{\n" +
                "  \"type\": \"K_MEANS_CLUSTERING\",\n" +
                "  \"collections\": [\n" +
                "    {\n" +
                "      \"name\": \"county_stats\",\n" +
                "      \"features\": [\n" +
                "        \"total_population\",\n" +
                "        \"median_household_income\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"kMeansClusteringRequest\": {\n" +
                "    \"clusterCount\": 10,\n" +
                "    \"maxIterations\": 100,\n" +
                "    \"resolution\": \"County\"\n" +
                "  }\n" +
                "}\n" +
                "\n";

        JsonModelRequest modelRequest = JsonModelRequest.newBuilder()
                .setJson(request)
                .build();
        Iterator<JsonModelResponse> jsonModelResponseIterator = jsonProxyBlockingStub.modelQuery(modelRequest);
        while (jsonModelResponseIterator.hasNext()) {
            JsonModelResponse jsonResponse = jsonModelResponseIterator.next();
            log.info("JSON Model Response: {}", jsonResponse.getJson());
        }
    }

    public SustainGrpc.SustainBlockingStub getSustainBlockingStub() {
        return sustainBlockingStub;
    }

    public JsonProxyGrpc.JsonProxyBlockingStub getJsonProxyBlockingStub() {
        return jsonProxyBlockingStub;
    }
}
