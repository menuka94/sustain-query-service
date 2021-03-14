package org.sustain.server;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.sustain.CompoundRequest;
import org.sustain.CompoundResponse;
import org.sustain.DirectRequest;
import org.sustain.DirectResponse;
import org.sustain.JsonModelRequest;
import org.sustain.JsonModelResponse;
import org.sustain.JsonProxyGrpc;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.SustainGrpc;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.handlers.CompoundQueryHandler;
import org.sustain.handlers.GrpcHandler;
import org.sustain.handlers.ModelHandler;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.handlers.DirectQueryHandler;
import org.sustain.handlers.EnsembleQueryHandler;
import org.sustain.util.Constants;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class SustainServer {

    private static final Logger log = LogManager.getLogger(SustainServer.class);
    private static JavaSparkContext sparkContext;

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        logEnvironment();
        initializeSparkContext();
        addClusterDependencyJars(sparkContext);

        final SustainServer server = new SustainServer();
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Logs the environment variables that the server was started with.
     */
    public static void logEnvironment() {
        log.info("\n\n--- Server Environment ---\n" +
                        "SERVER_HOST: {}\n" +
                        "SERVER_PORT: {}\n" +
                        "\n\n--- Database Environment ---\n" +
                        "DB_HOST: {}\n" +
                        "DB_PORT: {}\n" +
                        "DB_NAME: {}\n" +
                        "DB_USERNAME: {}\n" +
                        "DB_PASSWORD: {}\n", Constants.Server.HOST, Constants.Server.PORT, Constants.DB.HOST,
                Constants.DB.PORT, Constants.DB.NAME, Constants.DB.USERNAME, Constants.DB.PASSWORD);
    }

    /**
     * Initializes the Spark Context that will be used for all incoming queries to launch jobs on.
     * Configured to point at a MongoS instance for retrieving data.
     */
    private static void initializeSparkContext() {
        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("Sustain Query Service")
                .config("spark.mongodb.input.uri", String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT))
                .config("spark.mongodb.input.database", Constants.DB.NAME)
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }

    /**
     * Adds required dependency jars to the Spark Context member.
     * TODO: Add dependency jars to spark cluster workers at startup time
     */
    private static void addClusterDependencyJars(JavaSparkContext sparkContext) {
        String[] jarPaths = {
                "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
                "build/libs/spark-core_2.12-3.0.1.jar",
                "build/libs/spark-mllib_2.12-3.0.1.jar",
                "build/libs/spark-sql_2.12-3.0.1.jar",
                "build/libs/bson-4.0.5.jar",
                "build/libs/mongo-java-driver-3.12.5.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }
    }


    public void start() throws IOException {
        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
                .addService(new JsonProxyService())
                .addService(new SustainService())
                .build().start();
        log.info("Server started, listening on " + port);

        // Shutdown hook called when Ctrl+C is pressed
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

                try {
                    sparkContext.close();
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

    // SUSTAIN gRPC Server Implementation
    static class SustainService extends SustainGrpc.SustainImplBase {

        @Override
        public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {

            ModelHandler handler;
            ModelType type = request.getType();
            switch (type) {
                case LINEAR_REGRESSION:
                    log.info("Received a Linear Regression Model request");
                    handler = new RegressionQueryHandler(request, responseObserver, sparkContext);
                    break;
                case K_MEANS_CLUSTERING:
                    log.info("Received a K-Means Clustering Model request");
                    handler = new ClusteringQueryHandler(request, responseObserver, sparkContext);
                    break;
                case BISECTING_K_MEANS:
                    log.info("Received a Bisecting K-Means Model Request");
                    handler = new ClusteringQueryHandler(request, responseObserver, sparkContext);
                    break;
                case GAUSSIAN_MIXTURE:
                    log.info("Received a Gaussian Mixture Request");
                    handler = new ClusteringQueryHandler(request, responseObserver, sparkContext);
                    break;
                case R_FOREST_REGRESSION:
                    log.info("Received a Random Forest Regression Model request");
                    handler = new EnsembleQueryHandler(request, responseObserver, sparkContext);
                    break;
                case G_BOOST_REGRESSION:
                    log.info("Received a Gradient Boost Regression Model request");
                    handler = new EnsembleQueryHandler(request, responseObserver, sparkContext);
                    break;
                case LATENT_DIRICHLET_ALLOCATION:
                    log.info("Received a Latent Dirichlet Allocation Request");
                    handler = new ClusteringQueryHandler(request, responseObserver, sparkContext);
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
            GrpcHandler<CompoundRequest, CompoundResponse> handler = new CompoundQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

        @Override
        public void directQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
            GrpcHandler<DirectRequest, DirectResponse> handler = new DirectQueryHandler(request, responseObserver);
            handler.handleRequest();
        }

        /**
         * An example RPC method used to sanity-test the gRPC server manually, or unit-test it with JUnit.
         * @param request DirectRequest object containing a collection and query request.
         * @param responseObserver Response Stream for streaming back results.
         */
        @Override
        public void echoQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
            log.info("RPC method echoQuery() invoked; returning request query body");
            DirectResponse echoResponse = DirectResponse.newBuilder()
                    .setData(StringEscapeUtils.unescapeJavaScript(request.getQuery()))
                    .build();
            responseObserver.onNext(echoResponse);
            responseObserver.onCompleted();
        }
    }
}
