package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.OsmRequest;
import org.sustain.OsmResponse;
import org.sustain.controllers.OsmController;

import java.util.concurrent.LinkedBlockingQueue;

public class OsmQueryHandler extends GrpcHandler<OsmRequest, OsmResponse> {

    private static final Logger log = LogManager.getLogger(OsmQueryHandler.class);

    private boolean fetchingCompleted = false;

    public OsmQueryHandler(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        OsmRequest.Dataset dataset = request.getDataset();
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        new StreamWriter(queue, responseObserver).start();
        switch (dataset) {
            // query all OSM datasets
            case ALL:
                OsmController.getOsmData(request, OsmRequest.Dataset.LINES, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.MULTI_LINES, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.POINTS, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.MULTI_POLYGONS, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.OTHER, queue);

                fetchingCompleted = true;
                return;
            case UNRECOGNIZED:
                log.warn("Invalid OSM dataset");
        }

        // not ALL, query a single OSM dataset
        OsmController.getOsmData(request, dataset, queue);

        fetchingCompleted = true;
    }

    private class StreamWriter extends Thread {
        private volatile LinkedBlockingQueue<String> data;
        private StreamObserver<OsmResponse> responseObserver;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<OsmResponse> responseObserver) {
            this.data = data;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    responseObserver.onNext(OsmResponse.newBuilder().setResponse(datum).build());
                    count++;
                }
            }

            // if there is any data remaining in the queue after fetching is completed
            for (String datum : data) {
                responseObserver.onNext(OsmResponse.newBuilder().setResponse(datum).build());
                count++;
            }

            log.info("Streaming completed! No. of entries: " + count);
            responseObserver.onCompleted();
        }
    }
}
