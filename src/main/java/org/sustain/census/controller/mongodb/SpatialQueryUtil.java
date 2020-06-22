package org.sustain.census.controller.mongodb;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Geometry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.db.mongodb.DBConnection;

import java.util.ArrayList;

public class SpatialQueryUtil {
    private static final Logger log = LogManager.getLogger(SpatialQueryUtil.class);

    public static void findGeoWithin(String collectionName, Geometry geometry) {
        ArrayList<String> gisJoins = new ArrayList<>();
        log.info("findGeoWithin()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoWithin("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        while(cursor.hasNext()) {
            Document next = cursor.next();
            JsonObject jsonElement = JsonParser.parseString(next.toJson()).getAsJsonObject();
            JsonElement gisJoin = jsonElement.get("properties").getAsJsonObject().get("GISJOIN");
            log.info(gisJoin);
            gisJoins.add(gisJoin.toString());
        }
    }
}
