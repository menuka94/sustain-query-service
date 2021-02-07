package org.sustain.proximity;

import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;
import org.sustain.util.model.GeoJson;

import java.util.ArrayList;

public class SuperCollectionCreator {
    private static final Logger log = LogManager.getLogger(SuperCollectionCreator.class);

    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> counties = db.getCollection(Constants.GeoJsonCollections.COUNTIES_GEO);
        FindIterable<Document> documents = counties.find();

        ArrayList<String> gisJoins = new ArrayList<>();
        for (Document document : documents) {
            Gson gson = new Gson();
            GeoJson county = gson.fromJson(document.toJson(), GeoJson.class);
            String gisJoin = county.getProperties().getGisJoin();
            gisJoins.add(gisJoin);
        }

        log.info("Count: " + gisJoins.size());
    }
}
