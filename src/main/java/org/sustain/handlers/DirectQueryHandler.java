package org.sustain.handlers;

import com.mongodb.client.AggregateIterable;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bson.Document;
import org.sustain.DirectResponse;
import org.sustain.DirectRequest;
import org.sustain.mongodb.queries.AggregateQuery;

public class DirectQueryHandler extends GrpcHandler<DirectRequest, DirectResponse> {

    public static final Logger log = LogManager.getLogger(DirectQueryHandler.class);

    public DirectQueryHandler(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        log.info("Received query for collection '{}'", request.getCollection());
        long startTime = System.currentTimeMillis();

        try {
            // Submit MongoDB query
            AggregateIterable<Document> documents = new AggregateQuery().execute(request.getCollection(),
                    request.getQuery());

            // Initialize and start stream writer
            DirectStreamWriter streamWriter = new DirectStreamWriter(responseObserver, 1);
            streamWriter.start();

            // Process results
            long count = 0; 
            for (Document document : documents) {
                streamWriter.add(document);
                count += 1;
            }

            // Shutdown streamWriter and responseObserver
            streamWriter.stop(false);
            this.responseObserver.onCompleted();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Processed " + count + " document(s) from collection '"
                + request.getCollection() + "' in " + duration + "ms");
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    class DirectStreamWriter extends StreamWriter<Document, DirectResponse> {
        public DirectStreamWriter(
                StreamObserver<DirectResponse> responseObserver,
                int threadCount) {
            super(responseObserver, threadCount);
        }

        @Override
        public DirectResponse convert(Document document) {
            return DirectResponse.newBuilder()
                .setData(document.toJson())
                .build();
        }
    }
}
