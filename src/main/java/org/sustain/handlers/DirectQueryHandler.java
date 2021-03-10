package org.sustain.handlers;

import org.sustain.db.mongodb.DBConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bson.Document;

import org.json.JSONArray;

import org.sustain.DirectResponse;
import org.sustain.DirectRequest;

import java.util.ArrayList;

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
            // Connect to MongoDB
            MongoDatabase mongoDatabase = DBConnection.getConnection();
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(request.getCollection());

            // Construct MongoDB query object
            ArrayList<BasicDBObject> query = new ArrayList<>();

            // Build MongoDB
            JSONArray parsedQuery = new JSONArray(request.getQuery());
            for (int i = 0; i < parsedQuery.length(); i++) {
                BasicDBObject queryComponent = BasicDBObject.parse(parsedQuery.getJSONObject(i).toString());
                query.add(queryComponent);
            }

            // submit mongodb query
            AggregateIterable<Document> documents =
                mongoCollection.aggregate(query);

            // initialize and start stream writer
            DirectStreamWriter streamWriter =
                new DirectStreamWriter(responseObserver, 1);
            streamWriter.start();

            // process results
            long count = 0; 
            for (Document document : documents) {
                streamWriter.add(document);
                count += 1;
            }

            // shutdown streamWriter and responseObserver
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
