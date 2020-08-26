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
import org.sustain.census.Predicate;
import org.sustain.census.SpatialOp;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
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

    public static HashMap<String, String> findGeoWithin(String collectionName, Geometry geometry) {
        HashMap<String, String> geoJsons = new HashMap<>();
        log.info("findGeoWithin()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoWithin("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        int count = 0;
        while (cursor.hasNext()) {
            Document next = cursor.next();
            count++;
            //GeoJson geoJson = gson.fromJson(next.toJson(), GeoJson.class);
            String gisJoin =
                    JsonParser.parseString(next.toJson()).getAsJsonObject().get("properties").getAsJsonObject().get(Constants.GIS_JOIN).getAsString();
            geoJsons.put(gisJoin, next.toJson());
        }
        log.info("GEO WITHIN COUNT: " + count);

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

    public static HashMap<String, String> findGeoIntersects(String collectionName, Geometry geometry) {
        HashMap<String, String> geoJsons = new HashMap<>();
        log.info("findGeoIntersects()");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(Filters.geoIntersects("geometry", geometry));
        MongoCursor<Document> cursor = iterable.cursor();

        while (cursor.hasNext()) {
            Document next = cursor.next();
            //GeoJson geoJson = gson.fromJson(next.toJson(), GeoJson.class);
            String gisJoin =
                    JsonParser.parseString(next.toJson()).getAsJsonObject().get("properties").getAsJsonObject().get(Constants.GIS_JOIN).getAsString();
            geoJsons.put(gisJoin, next.toJson());
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
}
