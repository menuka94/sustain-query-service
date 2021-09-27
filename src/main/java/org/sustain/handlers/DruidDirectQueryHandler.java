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
            HttpRequest druidRequest = HttpRequest.newBuilder()
                .uri(new URI("http://lattice-123.cs.colostate.edu:8088/druid/v2"))
                .POST(HttpRequest.BodyPublishers.ofString(request.getQuery()))
                .build();

            // Druid provides responses in the form of JSON arrays.
            HttpResponse<String> druidResponse = httpClient.send(druidRequest, HttpResponse.BodyHandlers.ofString());
            DruidStreamWriter writer = new DruidStreamWriter(responseObserver, 1);
            JSONArray results = new JSONArray(druidResponse.body());
            for (int i = 0; i < results.length(); i++) {
                JSONObject row;
                if ((row = results.optJSONObject(i)) != null) {
                    writer.add(row);
                } else {
                    log.warn("Encountered an unexpected (non-object) row in result set: {}", row);
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Processed {} results in {} ms", results.length(), duration);
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
        public DruidDirectResponse convert(JSONObject queryResultRow) {
            return DruidDirectResponse.newBuilder()
                .setData(queryResultRow.toString())
                .build();
        }
    }
}
