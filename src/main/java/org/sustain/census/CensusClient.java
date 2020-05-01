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
        TARGET = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            log.error("Usage: CensusClient <resolutionKey> <latitude> <longitude> <aspect>\n" +
                    "Example: CensusClient state 24.5 -82 total_population");
            System.exit(0);
        }

        String resolutionKey = args[0];
        double latitude = Double.parseDouble(args[1]);
        double longitude = Double.parseDouble(args[2]);
        String aspect = args[3];

        log.info("Target: " + TARGET);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(TARGET).usePlaintext().build();

        try {
            CensusClient client = new CensusClient(channel);
            client.requestData(resolutionKey, latitude, longitude, aspect);
        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void requestData(String resolutionKey, double latitude, double longitude, String aspect) {
        log.info("Processing request (" + resolutionKey + ", " + latitude + ", " + longitude + ", " + aspect + ")");
        CensusRequest request = CensusRequest.newBuilder()
                .setResolutionKey(resolutionKey)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setAspect(aspect)
                .build();


        CensusResponse response = censusBlockingStub.getData(request);
        log.info("Response: " + response.getResponse());
    }
}
