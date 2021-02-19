package org.sustain.proximity;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.model.GeoJson;
import org.sustain.util.model.Geometry;

import java.util.HashMap;
import java.util.Set;

public class NumberOfEntitiesCalculator {
    private static final Logger log = LogManager.getLogger(NumberOfEntitiesCalculator.class);

    public static void main(String[] args) {
        calculateNoOfEntities("hospitals_geo", "county_geo");
    }

    private static void calculateNoOfEntities(String entitiesCollection, String resolutionCollectionName) {
        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> geoBoundaries = db.getCollection(resolutionCollectionName);
        MongoCollection<Document> entities = db.getCollection(entitiesCollection);
        FindIterable<Document> geoBoundariesDocs = geoBoundaries.find();

        for (Document geoBoundariesDoc : geoBoundariesDocs) {
            JsonElement jsonElement1 = JsonParser.parseString(geoBoundariesDoc.toJson());
            JsonElement geometry = jsonElement1.getAsJsonObject().get("geometry");
            JsonElement coordinates = geometry.getAsJsonObject().get("coordinates");
            log.info(coordinates);
            Document parsedCoordinates = Document.parse(coordinates.toString());
            entities.find(Filters.geoWithin(entitiesCollection, parsedCoordinates));
        }

    }
}
