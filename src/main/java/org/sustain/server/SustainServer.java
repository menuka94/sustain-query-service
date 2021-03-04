package org.sustain.server;

import com.google.protobuf.util.JsonFormat;
import com.mongodb.client.model.geojson.Geometry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CensusRequest;
import org.sustain.CensusResponse;
import org.sustain.ComparisonOperator;
import org.sustain.CompoundRequest;
import org.sustain.CompoundResponse;
import org.sustain.DatasetRequest;
import org.sustain.DatasetResponse;
import org.sustain.JsonModelRequest;
import org.sustain.JsonModelResponse;
import org.sustain.JsonProxyGrpc;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.OsmRequest;
import org.sustain.OsmResponse;
import org.sustain.Predicate;
import org.sustain.SpatialOp;
import org.sustain.SustainGrpc;
import org.sustain.SviRequest;
import org.sustain.SviResponse;
import org.sustain.handlers.CensusQueryHandler;
import org.sustain.db.queries.SpatialQueryUtil;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.handlers.GrpcHandler;
import org.sustain.handlers.OsmQueryHandler;
import org.sustain.handlers.DatasetQueryHandler;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.handlers.CompoundQueryHandler;
import org.sustain.controllers.SviController;
import org.sustain.util.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.sustain.db.queries.SpatialQueryUtil.getGeometryFromGeoJson;


public class SustainServer {
    private static final Logger log = LogManager.getLogger(SustainServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        logEnvironment();
        final SustainServer server = new SustainServer();
        server.start();
        server.blockUntilShutdown();
    }

    // Logs the environment variables that the server was started with.
    public static void logEnvironment() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n--- Server Environment ---\n");
        sb.append(String.format("SERVER_HOST: %s\n", Constants.Server.HOST));
        sb.append(String.format("SERVER_PORT: %s\n", Constants.Server.PORT));
        sb.append("\n--- Database Environment ---\n");
        sb.append(String.format("DB_HOST: %s\n", Constants.DB.HOST));
        sb.append(String.format("DB_PORT: %s\n", Constants.DB.PORT));
        sb.append(String.format("DB_NAME: %s\n", Constants.DB.NAME));
        sb.append(String.format("DB_USERNAME: %s\n", Constants.DB.USERNAME));
        sb.append(String.format("DB_PASSWORD: %s\n", Constants.DB.PASSWORD));
        log.info(sb.toString());
    }


    public void start() throws IOException {
        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
                .addService(new JsonProxyService())
                .addService(new SustainService())
                .build().start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            try {
                SustainServer.this.stop();
            } catch (InterruptedException e) {
                log.error("Error in stopping the server");
                e.printStackTrace();
            }
            log.warn("Server is shutting down");

        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdownNow() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    // JsonProxyService implementation
    static class JsonProxyService extends JsonProxyGrpc.JsonProxyImplBase {
        @Override
        public void modelQuery(JsonModelRequest request,
                               StreamObserver<JsonModelResponse> responseObserver) {
            ManagedChannel channel = null;

            try {
                // open grpc channel
                channel = ManagedChannelBuilder
                        .forAddress(Constants.Server.HOST,
                                Constants.Server.PORT)
                        .usePlaintext()
                        .build();

                // convert json to protobuf and service request
                JsonFormat.Parser parser = JsonFormat.parser();
                JsonFormat.Printer printer = JsonFormat.printer()
                        .includingDefaultValueFields()
                        .omittingInsignificantWhitespace();

                // create model request
                ModelRequest.Builder requestBuilder =
                        ModelRequest.newBuilder();
                parser.merge(request.getJson(), requestBuilder);

                // issue model request
                SustainGrpc.SustainBlockingStub blockingStub =
                        SustainGrpc.newBlockingStub(channel);

                Iterator<ModelResponse> iterator =
                        blockingStub.modelQuery(requestBuilder.build());

                // iterate over results
                while (iterator.hasNext()) {
                    ModelResponse response = iterator.next();

                    // build JsonModelRequest
                    String json = printer.print(response);
                    JsonModelResponse jsonResponse =
                            JsonModelResponse.newBuilder()
                                    .setJson(json)
                                    .build();

                    responseObserver.onNext(jsonResponse);
                }

                // send response
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("failed to evaluate", e);
                responseObserver.onError(e);
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }
    }

    // Server implementation
    static class SustainService extends SustainGrpc.SustainImplBase {
        @Override
        public void censusQuery(CensusRequest request, StreamObserver<CensusResponse> responseObserver) {
            GrpcHandler<CensusRequest, CensusResponse> handler = new CensusQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

        @Override
        public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
            GrpcHandler<ModelRequest, ModelResponse> handler;
            ModelType type = request.getType();
            switch (type) {
                case LINEAR_REGRESSION:
                    log.info("Received a Linear Regression Model request");
                    handler = new RegressionQueryHandler(request, responseObserver);
                    break;
                case K_MEANS_CLUSTERING:
                    log.info("Received a K-Means Clustering Model request");
                    handler = new ClusteringQueryHandler(request, responseObserver);
                    break;
                default:
                    responseObserver.onError(new Exception("Invalid Model Type"));
                    return;
            }

            handler.handleRequest();
            responseObserver.onCompleted();
        }

        @Override
        public void sviQuery(SviRequest request, StreamObserver<SviResponse> responseObserver) {
            Geometry geometry = getGeometryFromGeoJson(request.getRequestGeoJson());
            SpatialOp spatialOp = request.getSpatialOp();
            log.info("SVI Query{spatialOp: " + spatialOp + "}");
            HashMap<String, String> geos = new HashMap<>();
            switch (spatialOp) {
                case GeoWithin:
                    geos = SpatialQueryUtil.findGeoWithin(Constants.GeoJsonCollections.TRACTS_GEO,
                            geometry);
                    break;
                case GeoIntersects:
                    geos = SpatialQueryUtil.findGeoIntersects(Constants.GeoJsonCollections.TRACTS_GEO,
                            geometry);
                    break;
                case UNRECOGNIZED:
                    log.warn("Invalid spatial op: " + spatialOp);
            }
            ArrayList<String> gisJoins = new ArrayList<>(geos.keySet());
            for (String gisJoin : gisJoins) {
                String svi = SviController.getSviByGisJoin(gisJoin);
                if (svi != null) {
                    responseObserver.onNext(SviResponse.newBuilder()
                            .setData(svi)
                            .setResponseGeoJson(geos.get(gisJoin))
                            .build());
                }
            }

            responseObserver.onCompleted();
        }

        @Override
        public void osmQuery(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
            GrpcHandler<OsmRequest, OsmResponse> handler = new OsmQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

        @Override
        public void datasetQuery(DatasetRequest request, StreamObserver<DatasetResponse> responseObserver) {
            GrpcHandler<DatasetRequest, DatasetResponse> handler = new DatasetQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

        @Override
        public void compoundQuery(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
            GrpcHandler<CompoundRequest, CompoundResponse> handler = new CompoundQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

    }
}
