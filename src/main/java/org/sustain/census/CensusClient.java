package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private static String TARGET;
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            log.error("Usage: CensusClient <resolutionKey> <latitude> <longitude> <feature>\n" +
                    "Example: CensusClient " + Constants.CensusResolutions.STATE + " 24.5 -82 " + Constants.CensusFeatures.TOTAL_POPULATION + "\n" +
                    "Example: CensusClient " + Constants.CensusResolutions.STATE + " 24.5 -82 " + Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME + "\n" +
                    "Example: CensusClient " + Constants.CensusResolutions.COUNTY + " 24.5 -82 " + Constants.CensusFeatures.TOTAL_POPULATION + "\n" +
                    "Example: CensusClient " + Constants.CensusResolutions.COUNTY + " 24.5 -82 " + Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME);
            System.exit(0);
        }

        String resolutionKey = args[0];
        double latitude = Double.parseDouble(args[1]);
        double longitude = Double.parseDouble(args[2]);
        String feature = args[3];

        TARGET = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + TARGET);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(TARGET).usePlaintext().build();

        try {
            CensusClient client = new CensusClient(channel);
            client.requestData(resolutionKey, latitude, longitude, feature);
        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void requestData(String resolutionKey, double latitude, double longitude, String feature) {
        log.info("Processing request (" + resolutionKey + ", " + latitude + ", " + longitude + ", " + feature + ")");
        CensusRequest request = CensusRequest.newBuilder()
                .setResolution(resolutionKey)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setFeature(feature)
                .build();


        CensusResponse response = censusBlockingStub.getData(request);
        log.info("Response: " + response.get());
    }

    public static void getCentroidOfGeoHash(double lat, double lng) {

    }
}
