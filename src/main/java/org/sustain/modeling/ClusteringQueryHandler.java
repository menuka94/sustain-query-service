package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.KMeansClusteringRequest;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;

import java.util.ArrayList;

public class ClusteringQueryHandler {
    private static final Logger log = LogManager.getFormatterLogger(ClusteringQueryHandler.class);
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;


    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {
        KMeansClusteringRequest kMeansClusteringRequest = request.getKMeansClusteringRequest();
        int k = kMeansClusteringRequest.getClusterCount();
        int maxIterations = kMeansClusteringRequest.getMaxIterations();
        ArrayList<String> features = new ArrayList<>(kMeansClusteringRequest.getFeaturesList());
        log.info("\tk: " + k);
        log.info("\tmaxIterations: "+ maxIterations);
        log.info("\tfeatures:");
        for (String feature : features) {
            log.info("\t\t" + feature);
        }
    }
}
