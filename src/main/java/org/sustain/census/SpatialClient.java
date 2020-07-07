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
                "   \"type\":\"Feature\",\n" +
                "   \"properties\":{\n" +
                "\n" +
                "   },\n" +
                "   \"geometry\":{\n" +
                "      \"type\":\"polygon\",\n" +
                "      \"coordinates\":[\n" +
                "         [\n" +
                "            [\n" +
                "               -105.72280883789064,\n" +
                "               40.390488829277956\n" +
                "            ],\n" +
                "            [\n" +
                "               -105.72280883789064,\n" +
                "               40.75661990450192\n" +
                "            ],\n" +
                "            [\n" +
                "               -104.44976806640626,\n" +
                "               40.75661990450192\n" +
                "            ],\n" +
                "            [\n" +
                "               -104.44976806640626,\n" +
                "               40.390488829277956\n" +
                "            ],\n" +
                "            [\n" +
                "               -105.72280883789064,\n" +
                "               40.390488829277956\n" +
                "            ]\n" +
                "         ]\n" +
                "      ]\n" +
                "   }\n" +
                "}";

        SpatialRequest request = SpatialRequest.newBuilder()
                .setCensusFeature(CensusFeature.Race)
                .setCensusResolution(CensusResolution.Tract)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        //SpatialResponse spatialResponse = client.censusBlockingStub.spatialQuery(request);
        //List<SingleSpatialResponse> singleSpatialResponseList = spatialResponse.getSingleSpatialResponseList();
        //log.info("No. of records found: " + singleSpatialResponseList.size());
        //for (SingleSpatialResponse response : singleSpatialResponseList) {
        //    String data = response.getData();
        //    String singleGeoJson = response.getResponseGeoJson();
        //    log.info("data: " + data);
        //    log.info("geoJson: " + singleGeoJson);
        //    System.out.println();
        //}

        TargetedQueryRequest request1 = TargetedQueryRequest.newBuilder()
                .setResolution(CensusResolution.Tract)
                .setPredicate(
                        Predicate.newBuilder().setCensusFeature(CensusFeature.TotalPopulation)
                                .setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                                .setDecade(Decade._2010)
                                .setComparisonValue(10)
                                .build()
                )
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        TargetedQueryResponse targetedQueryResponse = client.censusBlockingStub.executeTargetedQuery(request1);
        List<SingleSpatialResponse> targetedResponseList = targetedQueryResponse.getSingleSpatialResponseList();
        log.info("No. of records found: " + targetedResponseList.size());
        for (SingleSpatialResponse response : targetedResponseList) {
            String data = response.getData();
            String singleGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + singleGeoJson);
            System.out.println();
        }
    }
}
