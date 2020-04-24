package org.sustain.census.db;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.CensusGrpc;
import org.sustain.census.CensusRequest;
import org.sustain.census.CensusResponse;

import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;
    private static final String TARGET = "localhost:50051"; // TODO: read from config file

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    private void requestData(String resolutionKey, int resolutionValue, String aspect) {
        log.info("Processing request (" + resolutionKey + ", " + resolutionValue + ", " + aspect + ")");
        CensusRequest request = CensusRequest.newBuilder()
                .setResolutionKey(resolutionKey)
                .setResolutionValue(resolutionValue)
                .setAspect(aspect)
                .build();


        CensusResponse response = censusBlockingStub.getData(request);
        log.info("Response: " + response.getResponse());

    }

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: CensusClient <resolutionKey> <resolutionValue> <aspect>\n" +
                    "Example: CensusClient state 01 total_population");
            System.exit(0);
        }

        String resolutionKey = args[0];
        int resolutionValue = Integer.parseInt(args[1]);
        String aspect = args[2];

        ManagedChannel channel = ManagedChannelBuilder.forTarget(TARGET).usePlaintext(true).build();

        try {
            CensusClient client = new CensusClient(channel);
            client.requestData(resolutionKey, resolutionValue, aspect);
        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
