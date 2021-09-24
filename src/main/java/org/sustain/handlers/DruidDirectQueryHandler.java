package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.DruidDirectRequest;
import org.sustain.DruidDirectResponse;

public class DruidDirectQueryHandler extends GrpcHandler<DruidDirectRequest, DruidDirectResponse> {
    public static final Logger log = LogManager.getLogger(DruidDirectQueryHandler.class);

    public DruidDirectQueryHandler(DruidDirectRequest request, StreamObserver<DruidDirectResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        log.info("Received a Druid query");

        try {

        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }
}
