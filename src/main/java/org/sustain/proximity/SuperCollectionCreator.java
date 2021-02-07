package org.sustain.proximity;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;
import org.sustain.util.model.GeoJson;

import java.util.ArrayList;

public class SuperCollectionCreator {
    private static final Logger log = LogManager.getLogger(SuperCollectionCreator.class);

    private static final ArrayList<String> gisJoins = new ArrayList<>();

    public static void main(String[] args) {
        getGisJoinsList();
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> countyStats = db.getCollection("county_stats");

        MongoCollection<Document> data = db.getCollection("county_total_population");
        for (String gisJoin : gisJoins) {
            BasicDBObject whereQuery = new BasicDBObject();
            whereQuery.put("GISJOIN", gisJoin);
            FindIterable<Document> documents = data.find(whereQuery);
            Document field = documents.first();
            int population = Integer.parseInt(field.get("2010_total_population").toString());
            countyStats.updateOne(Filters.eq("GISJOIN", gisJoin), Updates.set("total_population", population));
        }
    }

    private static void getGisJoinsList() {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> counties = db.getCollection(Constants.GeoJsonCollections.COUNTIES_GEO);
        FindIterable<Document> documents = counties.find();

        Gson gson = new Gson();
        for (Document document : documents) {
            GeoJson county = gson.fromJson(document.toJson(), GeoJson.class);
            String gisJoin = county.getProperties().getGisJoin();
            gisJoins.add(gisJoin);
        }

    }
}
