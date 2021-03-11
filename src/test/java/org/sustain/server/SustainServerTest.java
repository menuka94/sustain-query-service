package org.sustain.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.sustain.*;

import org.sustain.SustainGrpc;
import org.sustain.SustainGrpc.SustainBlockingStub;
import org.sustain.JsonProxyGrpc.JsonProxyBlockingStub;
import org.sustain.util.Constants;

/**
 * Tests gRPC calls and responses to the Sustain Server, as if a client were invoking them.
 * These tests are generally long-running, and should only be invoked on an as-need basis, instead of
 * as a prerequisite for builds. Furthermore, they are environment-specific, and will fail if the Sustain Server
 * is unable to reach the MongoDB or Spark clusters.
 */
public class SustainServerTest {

    private static final Logger log = LogManager.getLogger(SustainServerTest.class);
    private static final String TARGET = String.format("%s:%d", Constants.Server.HOST, Constants.Server.PORT);

    private static ManagedChannel channel;
    private static SustainBlockingStub sustainBlockingStub;
    private static JsonProxyBlockingStub jsonProxyBlockingStub;

    public SustainServerTest() {
        super();
    }

    /**
     * Example test template to test the echoQuery() RPC method.
     * Further tests should be implemented similar to this example structure.
     */
    @Tag("fast")
    @Test
    public void testExampleEchoQuery() {
        try {
            InputStream ioStream = getClass().getClassLoader().getResourceAsStream(
                    "requests/linear_regression_maca_v2_request.json");
            if (ioStream != null) {
                String testingResource = new String(ioStream.readAllBytes());
                DirectRequest testRequest = DirectRequest.newBuilder()
                        .setCollection("test_collection")
                        .setQuery(testingResource)
                        .build();

                Iterator<DirectResponse> responses = sustainBlockingStub.echoQuery(testRequest);
                while (responses.hasNext()) {
                    DirectResponse response = responses.next();
                    log.info(response.getData());
                    assertEquals(response.getData(), testingResource);
                }
            }

        } catch (NullPointerException e) {
            log.error("NullPtr: Failed to read testing resource file: ", e.getCause());
        } catch (IOException e) {
            log.error("Failed to read testing resource file: ", e.getCause());
        }
    }

    @Tag("slow")
    @Test
    public void testLinearRegressionModel() {
        executeJsonModelRequest("requests/linear_regression_maca_v2_request.json");
    }

    @Tag("slow")
    @Test
    public void testKMeansClusteringModel() {
        executeJsonModelRequest("requests/kmeans_clustering_county_stats_request.json");
    }

    @Tag("slow")
    @Test
    public void testBisectingKMeansClusteringModel() {
        executeJsonModelRequest("requests/bisecting_kmeans_clustering_county_stats_request.json");
    }

    @Tag("slow")
    @Test
    public void testLDAClusteringModel() {
        executeJsonModelRequest("requests/lra_clustering_county_stats_request.json");
    }

    @Tag("slow")
    @Test
    public void testGaussianMixtureClusteringModel() {
        executeJsonModelRequest("requests/gaussian_mixture_clustering_county_stats_request.json");
    }

    @Tag("slow")
    @Test
    public void testGBoostRegressionModel() {
        executeJsonModelRequest("requests/gboost_regression_maca_v2_request.json");
    }

    @Tag("slow")
    @Test
    public void testRForestRegressionModel() {
        executeJsonModelRequest("requests/rforest_regression_maca_v2_request.json");
    }

    /**
     * Tests the end-to-end Model Request functionality.
     * Due to the long-running nature of this test, it should not be included as a unit test, but rather manually
     * invoked and verified on an as-need basis.
     */
    public void executeJsonModelRequest(String resourceName) {
        try {
            InputStream ioStream = getClass().getClassLoader().getResourceAsStream(resourceName);
            if (ioStream != null) {
                String testingResource = new String(ioStream.readAllBytes());
                JsonModelRequest modelRequest = JsonModelRequest.newBuilder()
                        .setJson(testingResource)
                        .build();

                Iterator<JsonModelResponse> jsonModelResponseIterator = jsonProxyBlockingStub.modelQuery(modelRequest);
                while (jsonModelResponseIterator.hasNext()) {
                    JsonModelResponse jsonResponse = jsonModelResponseIterator.next();
                    log.info("JSON Model Response: {}", jsonResponse.getJson());
                }
            }

        } catch (NullPointerException e) {
            log.error("NullPtr: Failed to read testing resource file: ", e.getCause());
        } catch (IOException e) {
            log.error("Failed to read testing resource file: ", e.getCause());
        }
    }

    /**
     * Establishes a Managed gRPC Channel to the gRPC server running at the TARGET
     * specified location, and creates blocking stubs for both the Sustain and JsonProxy
     * Services.
     */
    @BeforeAll
    public static void beforeAllTests() {
        channel = ManagedChannelBuilder.forTarget(TARGET).usePlaintext().build();
        sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
        jsonProxyBlockingStub = JsonProxyGrpc.newBlockingStub(channel);
    }

    @AfterAll
    public static void afterAllTests() {
        shutdown();
    }

    /**
     * Shuts down the Managed gRPC Channel for this testing class.
     */
    public static void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Caught InterruptedException: " + e.getMessage());
            channel.shutdownNow();
        }
    }
}
