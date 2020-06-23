package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.List;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private final CensusGrpc.CensusBlockingStub censusBlockingStub;

    public SpatialClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        SpatialClient client = new SpatialClient(channel);
        final String geoJson = "{\n" +
                "        \"type\": \"Feature\",\n" +
                "        \"properties\": {},\n" +
                "        \"geometry\": {\n" +
                "            \"type\": \"Polygon\",\n" +
                "            \"coordinates\": [\n" +
                "                [\n" +
                "                    [\n" +
                "                        -74.23118591308594,\n" +
                "                        40.56389453066509\n" +
                "                    ],\n" +
                "                    [\n" +
                "                        -73.75259399414062,\n" +
                "                        40.56389453066509\n" +
                "                    ],\n" +
                "                    [\n" +
                "                        -73.75259399414062,\n" +
                "                        40.80965166748853\n" +
                "                    ],\n" +
                "                    [\n" +
                "                        -74.23118591308594,\n" +
                "                        40.80965166748853\n" +
                "                    ],\n" +
                "                    [\n" +
                "                        -74.23118591308594,\n" +
                "                        40.56389453066509\n" +
                "                    ]\n" +
                "                ]\n" +
                "            ]\n" +
                "        }\n" +
                "    }";
        SpatialRequest request = SpatialRequest.newBuilder()
                .setCensusFeature(CensusFeature.TotalPopulation)
                .setCensusResolution(CensusResolution.Tract)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        SpatialResponse spatialResponse = client.censusBlockingStub.spatialQuery(request);
        List<SingleSpatialResponse> singleSpatialResponseList = spatialResponse.getSingleSpatialResponseList();
        for (SingleSpatialResponse response : singleSpatialResponseList) {
            String data = response.getData();
            String singleGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + singleGeoJson);
            System.out.println();
        }
    }
}
