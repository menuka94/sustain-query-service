package org.sustain.handlers;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.SlidingWindowRequest;
import org.sustain.SlidingWindowResponse;
import org.sustain.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.Arrays;

public class SlidingWindowQueryHandler extends GrpcHandler<SlidingWindowRequest, SlidingWindowResponse> {
    private static final Logger log = LogManager.getFormatterLogger(SlidingWindowQueryHandler.class);

    public SlidingWindowQueryHandler(SlidingWindowRequest request,
                                     StreamObserver<SlidingWindowResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> covidCounty = db.getCollection("covid_county_formatted");

        int days = request.getDays();
        ArrayList<String> gisJoins = new ArrayList<>(request.getGisJoinList());
        for (String gisJoin : gisJoins) {
            AggregateIterable<Document> documents = processSingleGisJoin(gisJoin, days, covidCounty);
            for (Document document : documents) {
                System.out.println(document);
            }
        }
    }

    private AggregateIterable<Document> processSingleGisJoin(String gisJoin, int days,
                                                             MongoCollection<Document> mongoCollection) {
        log.info("Processing GISJOIN: {}", gisJoin);
        AggregateIterable<Document> aggregateIterable = mongoCollection.aggregate(Arrays.asList(
                new Document("$match", new Document("GISJOIN", gisJoin)),
                new Document("$sort", new Document("formatted_date", 1)),
                new Document("$group", new Document("_id", "$GISJOIN")
                        .append("prx", new Document("$push",
                                new Document("v", "$cases")
                                        .append("date", "$formatted_date")
                        ))),
                new Document(
                        "$addFields",
                        new Document("numDays", days)
                                .append("startDate",
                                        new Document("$arrayElemAt", Arrays.asList("$prx.date", 0)))
                ),
                new Document("$addFields", new Document("prx",
                        new Document("$map",
                                new Document("input", new Document("$range",
                                        Arrays.asList(0, new Document("$subtract",
                                                Arrays.asList(new Document("$size", "$prx"), days - 1)
                                        ))))
                                        .append("as", "z")
                                        .append("in", new Document("avg",
                                                new Document("$avg",
                                                        new Document("$slice", Arrays.asList("$prx.v", "$$z", days))
                                                ))
                                                .append("date", new Document("$arrayElemAt",
                                                        Arrays.asList("$prx.date", new Document("$add",
                                                                Arrays.asList("$$z", days - 1)))
                                                ))
                                        )
                        )
                )
                )
        ));

        return aggregateIterable;
    }
}
