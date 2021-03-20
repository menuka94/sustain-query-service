package org.sustain.proximity;

import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.util.Collections;

public class NearestInfrastructureQuerier {
    private static final String infraEntity = "electrical_substation";
    private static final String infrastructureDataset = String.format("%ss_geo", infraEntity);

    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> centers = db.getCollection("tract_centers");
        MongoCollection<Document> infraCollection = db.getCollection(infrastructureDataset);
        FindIterable<Document> documents = centers.find();

        int i = 0;
        // for each center
        for (Document document : documents) {
            if (i++ % 1000 == 0) {
                System.out.println(i);
            }
            double x = document.getDouble("x");
            double y = document.getDouble("y");
            String gisjoin = document.getString(Constants.GIS_JOIN);

            /**
             * db.new_stores.aggregate([
             *     { "$geoNear": {
             *         "near": {
             *             "type": "Point",
             *             "coordinates": [ -81.093699, 32.074673 ]
             *         },
             *         "maxDistance": 500 * 1609,
             *         "spherical": true,
             *         "distanceField": "distance",
             *         "distanceMultiplier": 0.000621371
             *     }}
             * ]).pretty()
             */
            double maxDistance = 500.0;
            int infraEntriesFound = 0;

            while (infraEntriesFound == 0) {
                BasicDBObject dbObject = new BasicDBObject("$geoNear",
                    new BasicDBObject("near", new BasicDBObject("type", "Point")
                        .append("coordinates", new double[]{x, y}))
                        .append("maxDistance", maxDistance)
                        .append("spherical", true)
                        .append("distanceField", "distance")
                );
                AggregateIterable<Document> infraEntitiesNearby =
                    infraCollection.aggregate(Collections.singletonList(dbObject));

                MongoCursor<Document> cursor = infraEntitiesNearby.cursor();
                if (cursor.hasNext()) {
                    Document next = cursor.next();
                    double distance =
                        JsonParser.parseString(next.toJson()).getAsJsonObject().get("distance").getAsDouble();
                    System.out.println("Distance: " + distance);
                    infraEntriesFound++;
                    // update proximity collection
                    MongoCollection<Document> tractProximity = db.getCollection("tract_proximity");
                    tractProximity.updateOne(Filters.eq("GISJOIN", gisjoin), new Document("$set", new Document(
                        String.format("nearest_%s", infraEntity), distance)));
                } else {
                    maxDistance += 500;
                    //System.out.println("Increasing max distance");
                }
            }
        }
    }
}