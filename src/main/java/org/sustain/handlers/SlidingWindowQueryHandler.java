package org.sustain.handlers;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;

import java.util.Arrays;

public class SlidingWindowQueryHandler {
    private static final Logger log = LogManager.getFormatterLogger(SlidingWindowQueryHandler.class);

    public static void main(String[] args) {
        int days = 7;
        MongoDatabase db = DBConnection.getConnection("localhost", 27017, "sustaindb");
        MongoCollection<Document> covid_county = db.getCollection("covid_county_formatted");
        AggregateIterable<Document> aggregateIterable = covid_county.aggregate(Arrays.asList(
                new Document("$match", new Document("GISJOIN", "G1700310")),
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

        for (Document document : aggregateIterable) {
            System.out.println(document);
        }
    }
}
