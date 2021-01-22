package org.sustain.dataModeling;

import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.db.mongodb.DBConnection;

public class ModelQueryHandler {
    private static final Logger log = LogManager.getLogger(ModelQueryHandler.class);
    private static final MongoDatabase db = DBConnection.getConnection();
    private final ModelRequest request;
    private final StreamObserver<ModelResponse> responseObserver;

    public ModelQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleClusteringRequest() {
        log.info("ModelRequest: " + request.getRequest());
        MongoCollection<Document> collection = db.getCollection("clustered_population_income_age");
        FindIterable<Document> documents = collection.find();
        MongoCursor<Document> cursor = documents.cursor();
        Gson gson = new Gson();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            ModelResponse response = ModelResponse.newBuilder()
                    .setResults(next.toJson())
                    .build();
            responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
    }
}