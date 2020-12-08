package org.sustain.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.sustain.DatasetRequest;
import org.sustain.db.mongodb.DBConnection;

public class GisJoinAggregator {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        FindIterable<Document> hospitals =
                db.getCollection(Constants.DATASETS.get(DatasetRequest.Dataset.HOSPITALS)).find();
        MongoCursor<Document> cursor = hospitals.cursor();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            String s = next.toJson();
            JsonObject sJson = JsonParser.parseString(s).getAsJsonObject();
            JsonArray coordinates = sJson.getAsJsonObject("geometry").get("coordinates").getAsJsonArray();
            Point point = new Point(new Position(coordinates.get(0).getAsDouble(), coordinates.get(1).getAsDouble()));
            System.out.println(point);
        }
    }
}
