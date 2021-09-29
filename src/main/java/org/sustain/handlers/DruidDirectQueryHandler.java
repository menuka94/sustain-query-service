package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sustain.DruidDirectRequest;
import org.sustain.DruidDirectResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class DruidDirectQueryHandler extends GrpcHandler<DruidDirectRequest, DruidDirectResponse> {
    public static final Logger log = LogManager.getLogger(DruidDirectQueryHandler.class);
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public DruidDirectQueryHandler(DruidDirectRequest request, StreamObserver<DruidDirectResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        log.info("Received a Druid query");
        long startTime = System.currentTimeMillis();

        try {
            // Druid does not have a native connector. This is the only way to query it.
            HttpRequest druidRequest = HttpRequest.newBuilder()
                .uri(new URI("http://lattice-123.cs.colostate.edu:8082/druid/v2"))
                .POST(HttpRequest.BodyPublishers.ofString(request.getQuery()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
            HttpResponse<String> druidResponse = httpClient.send(druidRequest, HttpResponse.BodyHandlers.ofString());

            DruidStreamWriter writer = new DruidStreamWriter(responseObserver, 1);
            writer.start();

            DruidQueryResult result = new DruidQueryResult(druidResponse.body());
            result.sendToWriter(writer);

            writer.stop(false);
            responseObserver.onCompleted();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Processed in {} ms", duration);
        } catch (JSONException e) {
            log.error("Deserialization of Druid response failed", e);
            responseObserver.onError(e);
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }

    }

    private static class DruidStreamWriter extends StreamWriter<JSONObject, DruidDirectResponse> {
        public DruidStreamWriter(
            StreamObserver<DruidDirectResponse> responseObserver,
            int threadCount
        ) {
            super(responseObserver, threadCount);
        }

        @Override
        public DruidDirectResponse convert(JSONObject object) {
            return DruidDirectResponse.newBuilder()
                .setData(object.toString())
                .build();
        }
    }

    private static class DruidQueryResult {
        // Results from a query are either a JSON object or JSON array.
        private JSONArray arrayResults;
        private JSONObject objectResults;
        private boolean isArray;

        public DruidQueryResult(String response) {
            try {
                arrayResults = new JSONArray(response);
                isArray = true;
            } catch (JSONException e) {
                objectResults = new JSONObject(response);
                isArray = false;
            }
        }

        public void sendToWriter(DruidStreamWriter writer) throws Exception {
            if (isArray) {
                for (int i = 0; i < arrayResults.length(); i++) {
                    JSONObject row;
                    if ((row = arrayResults.optJSONObject(i)) != null) {
                        writer.add(row);
                    } else {
                        log.warn("Encountered an unexpected (non-object) row in result set");
                    }
                }
            } else {
                writer.add(objectResults);
            }
        }
    }
}
