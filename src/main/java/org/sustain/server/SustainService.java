package org.sustain.server;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.handlers.*;
import org.sustain.handlers.GrpcHandler;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.util.Constants;

public class SustainService extends SustainGrpc.SustainImplBase {
    private static final Logger log = LogManager.getLogger(SustainService.class);
    private SparkManager sparkManager;

    public SustainService(SparkManager sparkManager) {
        this.sparkManager = sparkManager;
    }

    @Override
    public void slidingWindowQuery(SlidingWindowRequest request,
                                   StreamObserver<SlidingWindowResponse> responseObserver) {
        SlidingWindowQueryHandler handler = new SlidingWindowQueryHandler(request, responseObserver);
        log.info("Received a Sliding Window Query Request");
        handler.handleRequest();
    }

    @Override
    public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {

        GrpcHandler handler;
        ModelType type = request.getType();
        switch (type) {
            case LINEAR_REGRESSION:
                log.info("Received a Linear Regression Model request");
                handler = new RegressionQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case K_MEANS_CLUSTERING:
                log.info("Received a K-Means Clustering Model request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case BISECTING_K_MEANS:
                log.info("Received a Bisecting K-Means Model Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case GAUSSIAN_MIXTURE:
                log.info("Received a Gaussian Mixture Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case R_FOREST_REGRESSION:
                log.info("Received a Random Forest Regression Model request");
                handler = new EnsembleQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case G_BOOST_REGRESSION:
                log.info("Received a Gradient Boost Regression Model request");
                handler = new EnsembleQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                log.info("Received a Latent Dirichlet Allocation Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            default:
                responseObserver.onError(new Exception("Invalid Model Type"));
                return;
        }

        handler.handleRequest();
        responseObserver.onCompleted();
    }

    @Override
    public void compoundQuery(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
        GrpcHandler<CompoundRequest, CompoundResponse> handler = new CompoundQueryHandler(request,
            responseObserver);
        handler.handleRequest();
    }

    @Override
    public void countQuery(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        GrpcHandler<CountRequest, CountResponse> handler =
            new CountQueryHandler(request, responseObserver, this.sparkManager);
        handler.handleRequest();
    }

    @Override
    public void directQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        GrpcHandler<DirectRequest, DirectResponse> handler = new DirectQueryHandler(request, responseObserver);
        handler.handleRequest();
    }

    @Override
    public void druidDirectQuery(DruidDirectRequest request, StreamObserver<DruidDirectResponse> responseObserver) {
        GrpcHandler<DruidDirectRequest, DruidDirectResponse> handler = new DruidDirectQueryHandler(request, responseObserver);
        handler.handleRequest();
    }

    /**
     * An example RPC method used to sanity-test the gRPC server manually, or unit-test it with JUnit.
     *
     * @param request          DirectRequest object containing a collection and query request.
     * @param responseObserver Response Stream for streaming back results.
     */
    @Override
    public void echoQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        log.info("Received echoQuery request: {}", request.getQuery());
        String responseString = String.format("Request received on node %s: %s", Constants.Kubernetes.NODE_HOSTNAME,
                request.getQuery());

        DirectResponse echoResponse = DirectResponse.newBuilder()
            .setData(responseString)
            .build();
        responseObserver.onNext(echoResponse);
        responseObserver.onCompleted();
    }
}
