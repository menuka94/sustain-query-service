package org.sustain.census.db.mongodb;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.controller.mongodb.SpatialQueryUtil;
import org.sustain.census.db.Util;

import java.util.ArrayList;
import java.util.List;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static String host;
    private static String port;
    private static String db;
    private static MongoClient mongoClient;

    private static void initDatabaseProperties() {
        host = Util.getProperty(Constants.DB.HOST);
        log.info("Host: " + host);
        port = Util.getProperty(Constants.DB.PORT);
        log.info("Port: " + port);
        db = Constants.DB.DB_NAME;
    }

    public static MongoDatabase getConnection() {
        if (mongoClient == null) {
            log.info("Creating new connection to MongoDB");
            initDatabaseProperties();
            mongoClient = new MongoClient(host, Integer.parseInt(port));
        }
        log.info("Connecting to MongoDB instance: {" + host + ":" + port + "}");
        return mongoClient.getDatabase(db);
    }

    public static void main(String[] args) {
        String stringGeometry = " {\n" +
                "      \"type\": \"Feature\",\n" +
                "      \"properties\": {},\n" +
                "      \"geometry\": {\n" +
                "        \"type\": \"Polygon\",\n" +
                "        \"coordinates\": [\n" +
                "          [\n" +
                "            [\n" +
                "              -74.23118591308594,\n" +
                "              40.56389453066509\n" +
                "            ],\n" +
                "            [\n" +
                "              -73.75259399414062,\n" +
                "              40.56389453066509\n" +
                "            ],\n" +
                "            [\n" +
                "              -73.75259399414062,\n" +
                "              40.80965166748853\n" +
                "            ],\n" +
                "            [\n" +
                "              -74.23118591308594,\n" +
                "              40.80965166748853\n" +
                "            ],\n" +
                "            [\n" +
                "              -74.23118591308594,\n" +
                "              40.56389453066509\n" +
                "            ]\n" +
                "          ]\n" +
                "        ]\n" +
                "      }\n" +
                "    }";

        JsonObject geometry = JsonParser.parseString(stringGeometry).getAsJsonObject();
        JsonArray coordinatesArray =
                geometry.get("geometry").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsJsonArray();
        System.out.println("Size: " + coordinatesArray.size());
        List<Position> positions = new ArrayList<>();
        for (JsonElement jsonElement : coordinatesArray) {
            double latitude = jsonElement.getAsJsonArray().get(0).getAsDouble();
            double longitude = jsonElement.getAsJsonArray().get(1).getAsDouble();
            positions.add(new Position(latitude, longitude));
        }
        Geometry polygon = new Polygon(new PolygonCoordinates(positions));
        SpatialQueryUtil.findGeoWithin("tracts_geo", polygon);
    }
}
