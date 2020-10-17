package org.sustain.proximity;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;
import org.sustain.util.model.GeoJson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Data {
    public static void main(String[] args) throws IOException {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> tracts = db.getCollection(Constants.GeoJsonCollections.TRACTS_GEO);
        FindIterable<Document> documents = tracts.find();

        HashMap<String, Double[]> centers = new HashMap<>();
        // for each census tract
        for (Document document : documents) {
            Gson gson = new Gson();
            GeoJson tract = gson.fromJson(document.toJson(), GeoJson.class);
            JsonArray level1 = tract.getGeometry().getCoordinates();
            JsonArray level2 = level1.get(0).getAsJsonArray();
            JsonArray level3 = level2.get(0).getAsJsonArray();

            ArrayList<Double> x = new ArrayList<>();
            ArrayList<Double> y = new ArrayList<>();

            for (JsonElement array : level3) {
                x.add(array.getAsJsonArray().get(0).getAsDouble());
                y.add(array.getAsJsonArray().get(1).getAsDouble());
            }

            Double xMin = Collections.min(x);
            Double xMax = Collections.max(x);

            Double yMin = Collections.min(y);
            Double yMax = Collections.max(y);

            double xCenter = xMin + ((xMax - xMin) / 2);
            double yCenter = yMin + ((yMax - yMin) / 2);

            centers.put(tract.getProperties().getGisJoin(), new Double[]{xCenter, yCenter});
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("tract_centers.csv")));
        writer.write("GISJOIN, x, y");
        writer.newLine();

        for (String gisJoin : centers.keySet()) {
            Double[] center = centers.get(gisJoin);
            writer.write(gisJoin + "," + center[0] + "," + center[1]);
            writer.newLine();
        }

        System.out.println(centers.size());
    }
}
