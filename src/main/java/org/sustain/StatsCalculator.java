package org.sustain;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;

/**
 * To calculate aggregated statistics for states, counties, and tracts
 */
public class StatsCalculator {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> counties = db.getCollection("county_geo");

        FindIterable<Document> documents = counties.find();
        for (Document document : documents) {
            String gisJoin = document.getString("GISJOIN");
            System.out.println(gisJoin);
        }
    }
}
