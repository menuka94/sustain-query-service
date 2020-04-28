package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private static final String TARGET = "localhost:50051"; // TODO: read from config file
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: CensusClient <resolutionKey> <resolutionValue> <aspect>\n" +
                    "Example: CensusClient state 01 total_population");
            System.exit(0);
        }

        String resolutionKey = args[0];
        double latitude = Double.parseDouble(args[1]);
        double longitude = Double.parseDouble(args[2]);
        String aspect = args[2];

        ManagedChannel channel = ManagedChannelBuilder.forTarget(TARGET).usePlaintext(true).build();

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
