package org.sustain.querier;

import org.sustain.db.mongodb.DBConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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

        AtomicLong count = new AtomicLong(0);
        LinkedBlockingQueue<Document> queue = new LinkedBlockingQueue();
        ReentrantLock lock = new ReentrantLock();

        // start serialization thread
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        // retrieve next document
                        Document document = queue.take();

                        // initialize response
                        DirectResponse response =
                            DirectResponse.newBuilder()
                                .setData(document.toJson())
                                .build();

                        // send response
                        lock.lock();
                        try {
                            responseObserver.onNext(response);
                        } finally {
                            lock.unlock();
                        }

                        // decrement active count
                        count.decrementAndGet();
                    }
                } catch (InterruptedException e) {}
            }
        });

        thread.start();

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

            // iterate over query results
            AggregateIterable<Document> documents =
                mongoCollection.aggregate(query);
            long totalCount = 0; 
            for (Document document : documents) {
				queue.add(document);

				count.incrementAndGet();
                totalCount += 1;
            }

            // wait for worker threads to complete
            while (count.get() != 0) {
                Thread.sleep(50);
            }

            // stop worker thread
            thread.interrupt();

            this.responseObserver.onCompleted();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Processed " + totalCount
                + " document(s) from collection '"
                + request.getCollection() + "' in " + duration + "ms");
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }
}
