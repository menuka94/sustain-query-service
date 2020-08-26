package org.sustain.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.CensusFeature;
import org.sustain.census.CensusGrpc;
import org.sustain.census.CensusResolution;
import org.sustain.census.DatasetRequest;
import org.sustain.census.DatasetResponse;
import org.sustain.census.Decade;
import org.sustain.census.OsmRequest;
import org.sustain.census.OsmResponse;
import org.sustain.census.Predicate;
import org.sustain.census.SpatialOp;
import org.sustain.census.SpatialRequest;
import org.sustain.census.SpatialResponse;
import org.sustain.census.TargetedCensusRequest;
import org.sustain.census.TargetedCensusResponse;
import org.sustain.db.Util;
import org.sustain.util.Constants;
import org.sustain.util.SampleGeoJson;

import java.util.Iterator;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private CensusGrpc.CensusBlockingStub censusBlockingStub;

    public SpatialClient() {
        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        CensusGrpc.CensusBlockingStub censusBlockingStub = new SpatialClient().getCensusBlockingStub();

        //exampleSpatialQuery(censusBlockingStub, geoJson);
        //exampleTargetedQuery(censusBlockingStub, geoJson);
        //exampleOsmQuery(censusBlockingStub, SampleGeoJson.FORT_COLLINS);
        exampleDatasetQuery(DatasetRequest.Dataset.FIRE_STATIONS, censusBlockingStub, SampleGeoJson.MULTIPLE_STATES);
        //exampleSpatialQuery(CensusFeature.TotalPopulation, CensusResolution.Tract, censusBlockingStub,
        //        SampleGeoJson.MULTIPLE_STATES);
    }

    private static void exampleDatasetQuery(DatasetRequest.Dataset dataset,
                                            CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        DatasetRequest request = DatasetRequest.newBuilder()
                .setDataset(dataset)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();
        Iterator<DatasetResponse> datasetResponseIterator = censusBlockingStub.datasetQuery(request);
        int count = 0;
        while (datasetResponseIterator.hasNext()) {
            DatasetResponse response = datasetResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleOsmQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
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

    private static void exampleSpatialQuery(CensusFeature censusFeature, CensusResolution censusResolution,
                                            CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        SpatialRequest request = SpatialRequest.newBuilder()
                .setCensusFeature(censusFeature)
                .setCensusResolution(censusResolution)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        int count = 0;
        Iterator<SpatialResponse> spatialResponseIterator = censusBlockingStub.spatialQuery(request);
        while (spatialResponseIterator.hasNext()) {
            SpatialResponse response = spatialResponseIterator.next();
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
            count++;
        }
        log.info("Count: " + count);
    }

    private static void exampleTargetedQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        TargetedCensusRequest request = TargetedCensusRequest.newBuilder()
                .setResolution(CensusResolution.Tract)
                .setPredicate(
                        Predicate.newBuilder().setCensusFeature(CensusFeature.TotalPopulation)
                                .setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                                .setDecade(Decade._2010)
                                .setComparisonValue(2000)
                                .build()
                )
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        Iterator<TargetedCensusResponse> censusResponseIterator =
                censusBlockingStub.executeTargetedCensusQuery(request);
        while (censusResponseIterator.hasNext()) {
            TargetedCensusResponse response = censusResponseIterator.next();
            String data = response.getSpatialResponse().getData();
            String responseGeoJson = response.getSpatialResponse().getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
        }
    }

    public CensusGrpc.CensusBlockingStub getCensusBlockingStub() {
        return censusBlockingStub;
    }
}
