package org.sustain.census.controller.mongodb;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.Constants;
import org.sustain.census.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.List;

public class SpatialQueryUtil {
    private static final Logger log = LogManager.getLogger(SpatialQueryUtil.class);

    /**
     * @param geoJson of the following form;
     *                {
     *                "type": "Feature",
     *                "properties": {},
     *                "geometry": {
     *                "type": "Polygon",
     *                "coordinates": [
     *                [
     *                [x1, y1],
     *                [x2, y2]
     *                ]
     *                ]
     *                }
     * @return Geometry
     */
    public static Geometry constructPolygon(JsonObject geoJson) {
        JsonArray coordinatesArray =
                geoJson.get("geoJson").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsJsonArray();
        log.info("No. of points: " + coordinatesArray.size());
        List<Position> positions = new ArrayList<>();
        for (JsonElement jsonElement : coordinatesArray) {
            double latitude = jsonElement.getAsJsonArray().get(0).getAsDouble();
            double longitude = jsonElement.getAsJsonArray().get(1).getAsDouble();
            positions.add(new Position(latitude, longitude));
        }
        return new Polygon(new PolygonCoordinates(positions));
    }

    public static ArrayList<String> findGeoWithin(String collectionName, Geometry geometry) {
        ArrayList<String> gisJoins = new ArrayList<>();
        log.info("findGeoWithin()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoWithin("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        while (cursor.hasNext()) {
            Document next = cursor.next();
            JsonObject jsonElement = JsonParser.parseString(next.toJson()).getAsJsonObject();
            JsonElement gisJoin = jsonElement.get("properties").getAsJsonObject().get("GISJOIN");
            gisJoins.add(gisJoin.toString());
        }

        return gisJoins;
    }

    public static void main(String[] args) {

        String stringGeoJson = " {\n" +
                "      \"type\": \"Feature\",\n" +
                "      \"properties\": {},\n" +
                "      \"geoJson\": {\n" +
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

        JsonObject geoJson = JsonParser.parseString(stringGeoJson).getAsJsonObject();
        Geometry geometry = constructPolygon(geoJson);
        ArrayList<String> tractGisJoins = findGeoWithin(Constants.GeoJsonCollections.TRACTS_GEO, geometry);
        ArrayList<String> countyGisJoins = findGeoWithin(Constants.GeoJsonCollections.COUNTIES_GEO, geometry);
        log.info(tractGisJoins.size());
        log.info(countyGisJoins.size());
    }
}
