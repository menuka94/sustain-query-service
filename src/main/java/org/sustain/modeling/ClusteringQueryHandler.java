package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;

public class ClusteringQueryHandler {
    private static final Logger log = LogManager.getLogger(ClusteringQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;


    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {

    }
}
