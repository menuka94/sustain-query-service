package org.sustain.querier;

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
import org.sustain.Query;

import org.sustain.util.Constants;

import java.util.ArrayList;

public class DirectQueryHandler {
    public static final Logger log =
        LogManager.getLogger(DirectQueryHandler.class);

    private final StreamObserver<DirectResponse> responseObserver;

    public DirectQueryHandler(StreamObserver<DirectResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handleDirectQuery(DirectRequest request) {
        log.debug("Received query for collection '"
            + request.getCollection() + "'");
        long startTime = System.currentTimeMillis();

        try {
            // connect to mongodb
            MongoDatabase mongoDatabase = DBConnection.getConnection();
            MongoCollection mongoCollection = 
                mongoDatabase.getCollection(request.getCollection());

            // construct mongodb query object
            ArrayList<BasicDBObject> query =
                new ArrayList<BasicDBObject>();

            JSONArray parsedQuery = new JSONArray(request.getQuery());
            for (int i = 0; i < parsedQuery.length(); i++) {
                BasicDBObject queryComponent = new BasicDBObject()
                    .parse(parsedQuery.getJSONObject(i).toString());

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
