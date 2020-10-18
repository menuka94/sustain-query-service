package org.sustain.proximity;

import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.sustain.DatasetRequest;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;

public class NearestHospitalsQuerier {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> centers = db.getCollection("tract_centers");
        MongoCollection<Document> hospitals =
                db.getCollection(Constants.DATASETS.get(DatasetRequest.Dataset.HOSPITALS));
        FindIterable<Document> documents = centers.find();

        HashMap<String, ArrayList<String>> nearestHospitals = new HashMap<>();
        // for each center
        for (Document document : documents) {
            double x = document.getDouble("x");
            double y = document.getDouble("y");
            String gisjoin = document.getString("GISJOIN");
            ArrayList<String> hospitalIds = new ArrayList<>();

            System.out.println("GISJOIN: " + gisjoin);

            Point refPoint = new Point(new Position(x, y));

            double maxDistance = 1000.0;
            int hospitalsFound = 0;
            while (hospitalsFound == 0) {
                FindIterable<Document> hospitalsNearby = hospitals.find(Filters.near("geometry", refPoint, maxDistance,
                        0.0));
                MongoCursor<Document> cursor = hospitalsNearby.cursor();
                while (cursor.hasNext()) {
                    Document next = cursor.next();
                    String hospitalId =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("properties").getAsJsonObject().get("ID").getAsString();
                    hospitalIds.add(hospitalId);
                    System.out.println("Hospital ID: " + hospitalId);
                    hospitalsFound++;
                }
                maxDistance += 1000;
                //System.out.println("Increasing maxDistance: " + maxDistance);
            }

            System.out.println("hospitalsFound: " + hospitalsFound);
            nearestHospitals.put(gisjoin, hospitalIds);
            System.out.println();
        }
    }
}
