package org.sustain.dataModeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;

import java.util.concurrent.LinkedBlockingQueue;

public class ModelQueryHandler {
    private static final Logger log = LogManager.getLogger(ModelQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;
    private boolean fetchingCompleted = false;


    public ModelQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {
        String request = this.request.getRequest();
        // TODO:  process request

    }

    private class StreamWriter extends Thread {
        private volatile LinkedBlockingQueue<String> data;
        private StreamObserver<ModelResponse> responseObserver;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<ModelResponse> responseObserver) {
            this.data = data;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
        }
    }
}