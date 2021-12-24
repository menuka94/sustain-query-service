package org.sustain.handlers.query;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.SlidingWindowRequest;
import org.sustain.SlidingWindowResponse;
import org.sustain.mongodb.DBConnection;
import org.sustain.util.Constants;
import org.sustain.handlers.GrpcHandler;

import java.util.ArrayList;
import java.util.Arrays;

public class SlidingWindowQueryHandler extends GrpcHandler<SlidingWindowRequest, SlidingWindowResponse> {
    private static final Logger log = LogManager.getLogger(SlidingWindowQueryHandler.class);

    public SlidingWindowQueryHandler(SlidingWindowRequest request,
                                     StreamObserver<SlidingWindowResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        logRequest(request);
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(request.getCollection());

        int days = request.getDays();
        String feature = request.getFeature();
        ArrayList<String> gisJoins = new ArrayList<>(request.getGisJoinsList());
        for (String gisJoin : gisJoins) {
            AggregateIterable<Document> documents = processSingleGisJoin(gisJoin, feature, days, collection);
            SlidingWindowResponse.Builder responseBuilder = SlidingWindowResponse.newBuilder();
            responseBuilder.setGisJoin(gisJoin);
            ArrayList<String> movingAverages = new ArrayList<>();
            for (Document document : documents) {
                movingAverages.add(document.toJson());
            }
            responseBuilder.addAllMovingAverages(movingAverages);
            responseObserver.onNext(responseBuilder.build());
        }
        log.info("Completed Sliding Window Query!");
        responseObserver.onCompleted();
    }

    private AggregateIterable<Document> processSingleGisJoin(String gisJoin, String feature, int days,
                                                             MongoCollection<Document> mongoCollection) {
        log.info("Processing GISJOIN: {}", gisJoin);
        // The following aggregation query is based on the raw MongoDB query found at https://pastebin.com/HUciUXZW
        AggregateIterable<Document> aggregateIterable = mongoCollection.aggregate(Arrays.asList(
            new Document("$match", new Document(Constants.GIS_JOIN, gisJoin)),
            new Document("$sort", new Document("formatted_date", 1)),
            new Document("$group", new Document("_id", String.format("$%s", Constants.GIS_JOIN))
                .append("movingAverages", new Document("$push",
                    new Document("value", String.format("$%s", feature))
                        .append("date", "$formatted_date")
                ))),
            new Document(
                "$addFields",
                new Document("numDays", days)
                    .append("startDate",
                        new Document("$arrayElemAt", Arrays.asList("$movingAverages.date", 0)))
            ),
            new Document("$addFields", new Document("movingAverages",
                new Document("$map",
                    new Document("input", new Document("$range",
                        Arrays.asList(0, new Document("$subtract",
                            Arrays.asList(new Document("$size", "$movingAverages"), days - 1)
                        ))))
                        .append("as", "computedValue")
                        .append("in", new Document("avg",
                            new Document("$avg",
                                new Document("$slice", Arrays.asList("$movingAverages.value", "$$computedValue", days))
                            ))
                            .append("date", new Document("$arrayElemAt",
                                Arrays.asList("$movingAverages.date", new Document("$add",
                                    Arrays.asList("$$computedValue", days - 1)))
                            ))
                        )
                )
            )
            )
        ));

        return aggregateIterable;
    }
}
