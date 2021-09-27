package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

        try {
            HttpRequest druidRequest = HttpRequest.newBuilder()
                .uri(new URI("http://lattice-123.cs.colostate.edu:8088/druid/v2"))
                .POST(HttpRequest.BodyPublishers.ofString(request.getQuery()))
                .build();

            HttpResponse<String> druidResponse = httpClient.send(druidRequest, HttpResponse.BodyHandlers.ofString());


        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    private static class DruidStreamWriter extends StreamWriter<String, DruidDirectResponse> {
        public DruidStreamWriter(
            StreamObserver<DruidDirectResponse> responseObserver,
            int threadCount
        ) {
            super(responseObserver, threadCount);
        }

        @Override
        public DruidDirectResponse convert(String s) {
            return DruidDirectResponse.newBuilder().setData(s).build();
        }
    }
}
