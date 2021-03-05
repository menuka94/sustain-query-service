package org.sustain;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;

/**
 * To calculate aggregated statistics for states, counties, and tracts
 */
public class StatsCalculator {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> counties = db.getCollection("county_geo");

        String infraCollectionName = "dams";
        MongoCollection<Document> infraCollection = db.getCollection(infraCollectionName + "_geo");
        String resolution = "county";

        String statsCollectionName = resolution + "_stats";
        MongoCollection<Document> stats = db.getCollection(statsCollectionName);

        int totalInfraEntities = 0;
        FindIterable<Document> documents = counties.find();
        for (Document document : documents) {
            String gisJoin = document.getString("GISJOIN");
            FindIterable<Document> matches = infraCollection.find(Filters.eq("GISJOIN_" + resolution, gisJoin));
            int infraEntityCount = 0;
            for (Document match : matches) {
                infraEntityCount++;
            }
            totalInfraEntities += infraEntityCount;

            stats.updateOne(Filters.eq("GISJOIN", gisJoin), new Document("$set", new Document(
                    "no_of_" + infraCollectionName, infraEntityCount
            )));
            System.out.println(gisJoin + ": " + infraEntityCount);

        }
        System.out.println("Total " + infraCollectionName + ": " + totalInfraEntities);
    }
}
