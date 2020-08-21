package org.sustain.census;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

public class CensusQueryHandler {
    private static final Logger log = LogManager.getLogger(CensusQueryHandler.class);

    private final SpatialRequest request;
    private final StreamObserver<SpatialResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public CensusQueryHandler(SpatialRequest request, StreamObserver<SpatialResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleCensusQuery() {

    }

    private class StreamWriter extends Thread {
        private volatile LinkedBlockingQueue<String> data;
        private StreamObserver<SpatialResponse> responseObserver;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<SpatialResponse> responseObserver) {
            this.data = data;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    responseObserver.onNext(SpatialResponse.newBuilder()
                            .setData(datum)
                            .setResponseGeoJson("")
                            .build());
                }
            }

            for (String datum: data) {

            }
        }
    }
}
