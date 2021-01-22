package org.sustain.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CensusFeature;
import org.sustain.CensusRequest;
import org.sustain.CensusResolution;
import org.sustain.CensusResponse;
import org.sustain.DatasetRequest;
import org.sustain.DatasetResponse;
import org.sustain.Decade;
import org.sustain.OsmRequest;
import org.sustain.OsmResponse;
import org.sustain.Predicate;
import org.sustain.SpatialOp;
import org.sustain.SustainGrpc;
import org.sustain.SviRequest;
import org.sustain.SviResponse;
import org.sustain.TargetedCensusRequest;
import org.sustain.TargetedCensusResponse;
import org.sustain.db.Util;
import org.sustain.util.Constants;
import org.sustain.util.SampleGeoJson;

import java.util.Iterator;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private SustainGrpc.SustainBlockingStub sustainBlockingStub;

    public SpatialClient() {
        String target = Util.getProperty(Constants.Server.HOST) + ":" + 30001;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        SustainGrpc.SustainBlockingStub sustainBlockingStub = new SpatialClient().getSustainBlockingStub();

        //exampleSpatialQuery(sustainBlockingStub, geoJson);
        //exampleTargetedQuery(sustainBlockingStub, geoJson);
        //exampleOsmQuery(sustainBlockingStub, SampleGeoJson.FORT_COLLINS);
        exampleDatasetQuery(DatasetRequest.Dataset.FIRE_STATIONS, sustainBlockingStub, SampleGeoJson.MULTIPLE_STATES);
        //exampleCensusQuery(CensusFeature.TotalPopulation, CensusResolution.County, sustainBlockingStub,
        //        SampleGeoJson.COLORADO);
        //exampleSviQuery(SampleGeoJson.COLORADO, SpatialOp.GeoIntersects, sustainBlockingStub);
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

    private static void exampleCensusQuery(CensusFeature censusFeature, CensusResolution censusResolution,
                                           SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
        CensusRequest request = CensusRequest.newBuilder()
                .setCensusFeature(censusFeature)
                .setCensusResolution(censusResolution)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        int count = 0;
        Iterator<CensusResponse> CensusResponseIterator = censusBlockingStub.censusQuery(request);
        while (CensusResponseIterator.hasNext()) {
            CensusResponse response = CensusResponseIterator.next();
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
            count++;
        }
        log.info("Count: " + count);
    }

    private static void exampleTargetedQuery(SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
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
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
        }
    }

    public SustainGrpc.SustainBlockingStub getSustainBlockingStub() {
        return sustainBlockingStub;
    }
}
