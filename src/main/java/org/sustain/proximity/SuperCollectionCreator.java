package org.sustain.proximity;

import com.google.gson.Gson;
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

        MongoCollection<Document> data = db.getCollection("county_median_household_income");
        for (String gisJoin : gisJoins) {
            FindIterable<Document> documents = data.find(Filters.eq("GISJOIN", gisJoin));
            Document field = documents.first();
            if (field != null) {
                double population = Double.parseDouble(field.get("2010_median_household_income").toString());
                countyStats.updateOne(Filters.eq("GISJOIN", gisJoin), Updates.set("median_household_income", population));
            } else {
                log.info("No data");
            }
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
