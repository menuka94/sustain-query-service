package org.sustain.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.HashMap;

import static com.mongodb.client.model.Filters.eq;

public class GisJoinAggregator {
    private static final Logger log = LogManager.getLogger(GisJoinAggregator.class);

    public static void main(String[] args) {
        updateCollection("hospitals", "county");
        updateCollection("hospitals", "tract");
    }

    public static void updateCollection(String collectionName, String resolution) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> documents = collection.find();
        MongoCursor<Document> cursor = documents.cursor();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            String s = next.toJson();
            JsonObject sJson = JsonParser.parseString(s).getAsJsonObject();
            JsonArray coordinates = sJson.getAsJsonObject("geometry").get("coordinates").getAsJsonArray();
            String documentId = next.getObjectId("_id").toString();
            Point point = new Point(new Position(coordinates.get(0).getAsDouble(), coordinates.get(1).getAsDouble()));
            HashMap<String, String> county_geo = SpatialQueryUtil.findGeoIntersects(resolution + "_geo", point);

            int count = county_geo.size();
            if (count != 1) {
                log.info("Count is " + count);
            } else {
                String gisJoin = new ArrayList<>(county_geo.keySet()).get(0);
                log.debug(gisJoin);
                collection.updateOne(eq("_id", new ObjectId(documentId)), new Document("$set", new Document(
                        "GISJOIN_" + resolution, gisJoin)));
            }
            log.info("Completed!");
        }
    }
}
