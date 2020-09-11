package org.sustain.census;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.TargetedCensusRequest;
import org.sustain.TargetedCensusResponse;

public class TargetedCensusQueryHandler {
    public static final Logger log = LogManager.getLogger(TargetedCensusQueryHandler.class);

    private final TargetedCensusRequest request;
    private final StreamObserver<TargetedCensusResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public TargetedCensusQueryHandler(TargetedCensusRequest request,
                                      StreamObserver<TargetedCensusResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleTargetedCensusQuery() {

    }
}
