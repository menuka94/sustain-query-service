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
import org.sustain.DirectRequest;
import org.sustain.DirectResponse;
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
import org.sustain.datasets.handlers.CensusQueryHandler;
import org.sustain.datasets.controllers.SpatialQueryUtil;
import org.sustain.modeling.ClusteringQueryHandler;
import org.sustain.datasets.handlers.OsmQueryHandler;
import org.sustain.datasets.handlers.DatasetQueryHandler;
import org.sustain.modeling.RegressionQueryHandler;
import org.sustain.querier.DirectQueryHandler;
import org.sustain.querier.CompoundQueryHandler;
import org.sustain.datasets.controllers.SviController;
import org.sustain.util.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.sustain.datasets.controllers.SpatialQueryUtil.getGeometryFromGeoJson;


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
        log.info("--- Server Environment ---");
        log.info("SERVER_HOST: " + Constants.Server.HOST);
        log.info("SERVER_PORT: " + Constants.Server.PORT);
        log.info("--- Database Environment ---");
        log.info("DB_HOST: " + Constants.DB.HOST);
        log.info("DB_PORT: " + Constants.DB.PORT);
        log.info("DB_NAME: " + Constants.DB.NAME);
        log.info("DB_USERNAME: " + Constants.DB.USERNAME);
        log.info("DB_PASSWORD: " + Constants.DB.PASSWORD);
        log.info("SPARK_MASTER: " + Constants.Spark.MASTER);
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
            CensusQueryHandler handler = new CensusQueryHandler(request, responseObserver);
            handler.handleCensusQuery();
        }

        @Override
        public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
            ModelType type = request.getType();
            switch (type) {
                case LINEAR_REGRESSION:
                    log.info("Received a Linear Regression Model request");
                    RegressionQueryHandler regressionHandler = new RegressionQueryHandler(request, responseObserver);
                    regressionHandler.handleQuery();
                    break;
                case K_MEANS_CLUSTERING:
                    log.info("Received a K-Means Clustering Model request");
                    ClusteringQueryHandler clusteringHandler = new ClusteringQueryHandler(request, responseObserver);
                    clusteringHandler.handleQuery();
                    break;
                case UNRECOGNIZED:
                    responseObserver.onError(new Exception("Invalid Model Type"));
            }
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
            OsmQueryHandler handler = new OsmQueryHandler(request, responseObserver);
            handler.handleOsmQuery();
        }

        @Override
        public void datasetQuery(DatasetRequest request, StreamObserver<DatasetResponse> responseObserver) {
            DatasetQueryHandler handler = new DatasetQueryHandler(request, responseObserver);
            handler.handleDatasetQuery();
        }

        @Override
        public void compoundQuery(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
            CompoundQueryHandler handler = new CompoundQueryHandler(responseObserver);
            handler.handleCompoundQuery(request, true);
        }

        @Override
        public void directQuery(DirectRequest request,
                StreamObserver<DirectResponse> responseObserver) {
            DirectQueryHandler handler =
                new DirectQueryHandler(responseObserver);
            handler.handleDirectQuery(request);
        }
    }   // end of Server implementation
}
