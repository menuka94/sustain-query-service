package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;
    private final ClientHelper clientHelper;

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
        clientHelper = new ClientHelper(censusBlockingStub);
    }

    public static void main(String[] args) {

    }
}
