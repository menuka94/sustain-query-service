package org.sustain.census.controller;

import com.google.gson.Gson;
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
import org.bson.conversions.Bson;
import org.sustain.util.Constants;
import org.sustain.census.Predicate;
import org.sustain.census.SpatialOp;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.model.GeoJson;

import java.util.ArrayList;
import java.util.List;

public class SpatialQueryUtil {
    private static final Logger log = LogManager.getLogger(SpatialQueryUtil.class);
    private static final Gson gson = new Gson();

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
                geoJson.get("geometry").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsJsonArray();
        log.info("No. of points: " + coordinatesArray.size());
        List<Position> positions = new ArrayList<>();
        for (JsonElement jsonElement : coordinatesArray) {
            double latitude = jsonElement.getAsJsonArray().get(0).getAsDouble();
            double longitude = jsonElement.getAsJsonArray().get(1).getAsDouble();
            positions.add(new Position(latitude, longitude));
        }
        return new Polygon(new PolygonCoordinates(positions));
    }

    public static ArrayList<GeoJson> findGeoWithin(String collectionName, Geometry geometry) {
        ArrayList<GeoJson> geoJsons = new ArrayList<>();
        log.info("findGeoWithin()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoWithin("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        // sub-query with iterable - the following exception is thrown
        // org.bson.BsonMaximumSizeExceededException: Document size of 78569244 is larger than maximum of 16793600.
        /*
        MongoCollection<Document> county_total_population = db.getCollection("county_total_population");
        FindIterable<Document> gisjoin = county_total_population.find(Filters.in("GISJOIN", iterable));
        MongoCursor<Document> populationCursor = gisjoin.cursor();
        int i = 0;
        while(populationCursor.hasNext()) {
            Document next = populationCursor.next();
            i++;
        }
        log.info("TEMP COUNT: " + i);
        */

        while (cursor.hasNext()) {
            Document next = cursor.next();
            GeoJson geoJson = gson.fromJson(next.toJson(), GeoJson.class);
            geoJsons.add(geoJson);
        }

        return geoJsons;
    }

    public static String getGeoFromGisJoin(String resolution, String gisJoin) {
        MongoDatabase db = DBConnection.getConnection();
        String collectionName = resolution + "_geo";
        MongoCollection<Document> collection = db.getCollection(collectionName);
        Document first = collection.find(Filters.eq("properties." + Constants.GIS_JOIN, gisJoin)).first();
        if (first != null && !first.isEmpty()) {
            return first.toJson();
        } else {
            return "";
        }
    }

    public static ArrayList<GeoJson> findGeoIntersects(String collectionName, Geometry geometry) {
        ArrayList<GeoJson> geoJsons = new ArrayList<>();
        log.info("findGeoIntersects()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoIntersects("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        while (cursor.hasNext()) {
            Document next = cursor.next();
            JsonObject jsonElement = JsonParser.parseString(next.toJson()).getAsJsonObject();
            GeoJson geoJson = gson.fromJson(next.toJson(), GeoJson.class);
            geoJsons.add(geoJson);
        }

        return geoJsons;
    }

    public static Bson getFilterOpFromComparisonOp(Predicate.ComparisonOperator comparisonOperator,
                                                   String comparisonField, double comparisonValue) {
        switch (comparisonOperator) {
            case EQUAL:
                return Filters.eq(comparisonField, comparisonValue);
            case LESS_THAN:
                return Filters.lt(comparisonField, comparisonValue);
            case LESS_THAN_OR_EQUAL:
                return Filters.lte(comparisonField, comparisonValue);
            case GREATER_THAN_OR_EQUAL:
                return Filters.gte(comparisonField, comparisonValue);
            case GREATER_THAN:
                return Filters.gt(comparisonField, comparisonValue);
            case UNRECOGNIZED:
                log.warn("Unrecognized comparison operator: " + comparisonOperator.toString());
        }
        return null;
    }

    public static Bson getSpatialOp(SpatialOp spatialOp, Geometry geometry) {
        switch (spatialOp) {
            case GeoWithin:
                return Filters.geoWithin("geometry", geometry);
            case GeoIntersects:
                return Filters.geoIntersects("geometry", geometry);
            case UNRECOGNIZED:
                log.warn("Unrecognized Spatial Operator");
        }
        return null;
    }

    public static Geometry getGeometryFromGeoJson(String requestGeoJson) {
        JsonObject inputGeoJson = JsonParser.parseString(requestGeoJson).getAsJsonObject();
        return SpatialQueryUtil.constructPolygon(inputGeoJson);
    }

    public static void main(String[] args) {
        final String stringGeoJson = " {\n" +
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

        JsonObject geoJson = JsonParser.parseString(stringGeoJson).getAsJsonObject();
        Geometry geometry = constructPolygon(geoJson);
        ArrayList<GeoJson> tractGeoWithin = findGeoWithin(Constants.GeoJsonCollections.TRACTS_GEO, geometry);
        ArrayList<GeoJson> countyGeoWithin = findGeoWithin(Constants.GeoJsonCollections.COUNTIES_GEO, geometry);

        for (GeoJson json : tractGeoWithin) {
            log.info("GIS JOIN: " + json.getProperties().getGisJoin());
        }
    }
}
