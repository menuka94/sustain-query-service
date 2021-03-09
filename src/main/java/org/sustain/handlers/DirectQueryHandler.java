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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DirectQueryHandler extends GrpcHandler<DirectRequest, DirectResponse> {

    public static final Logger log = LogManager.getLogger(DirectQueryHandler.class);

    public DirectQueryHandler(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        log.info("Received query for collection '{}'", request.getCollection());
        long startTime = System.currentTimeMillis();

        AtomicLong count = new AtomicLong(0);
        LinkedBlockingQueue<Document> queue = new LinkedBlockingQueue<>();
        ReentrantLock lock = new ReentrantLock();

        // Start serialization thread
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        // Retrieve next document
                        Document document = queue.take();

                        // Initialize response
                        DirectResponse response = DirectResponse.newBuilder()
                                .setData(document.toJson())
                                .build();

                        // Send response
                        lock.lock();
                        try {
                            responseObserver.onNext(response);
                        } finally {
                            lock.unlock();
                        }

                        // Decrement active count
                        count.decrementAndGet();
                    }
                } catch (InterruptedException e) {
                    log.info("Thread interrupted");
                }
            }
        });

        // Start the Thread to process/stream Document results back to the gRPC client
        thread.start();

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

            // Iterate over query results
            AggregateIterable<Document> documents = mongoCollection.aggregate(query);
            long totalCount = 0;
            for (Document document : documents) {
                queue.add(document);
                count.incrementAndGet();
                totalCount += 1;
            }

            // Wait for worker threads to complete
            while (count.get() != 0) {
                Thread.sleep(50);
            }

            // Stop worker thread and end gRPC stream
            thread.interrupt();
            this.responseObserver.onCompleted();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Processed {} document(s) from collection '{}' in {} ms", totalCount,
                    request.getCollection(), duration);
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }
}
