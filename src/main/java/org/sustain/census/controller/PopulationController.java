package org.sustain.census.controller;

import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Geometry;
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

public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);

    public static ArrayList<String> getTotalPopulationResults(String resolution, ArrayList<String> geoJsonList) {
        return getPopulationResults(resolution, geoJsonList, Constants.CensusFeatures.TOTAL_POPULATION);
    }

    public static ArrayList<String> getPopulationByAgeResults(String resolution, ArrayList<String> geoJsonList) {
        return getPopulationResults(resolution, geoJsonList, Constants.CensusFeatures.POPULATION_BY_AGE);
    }

    // common method used for fetching both 'total_population' and 'population_by_age'
    private static ArrayList<String> getPopulationResults(String resolution, ArrayList<String> geoJsonList,
                                                          String collectionName) {
        ArrayList<String> populationResults = new ArrayList<>();
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + collectionName);
        FindIterable<Document> iterable = collection.find(Filters.in("GISJOIN", geoJsonList));
        MongoCursor<Document> cursor = iterable.cursor();
        while(cursor.hasNext()) {
            Document next = cursor.next();
            populationResults.add(next.toJson());
        }
        return populationResults;

    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution,
                                                            Predicate.ComparisonOperator comparisonOp,
                                                            double comparisonValue, Geometry geometry,
                                                            SpatialOp spatialOp) {
        log.info("fetchTargetedInfo({decade: " + decade + ", resolution: " + resolution + ", comparisonOp: " + comparisonOp + "," +
                " comparisonValue: " + comparisonValue + ", spatialOp: " + spatialOp + " })");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + Constants.CensusFeatures.TOTAL_POPULATION);
        String comparisonField = decade + "_" + Constants.CensusFeatures.TOTAL_POPULATION;
        log.info("comparisonField: " + comparisonField);
        Bson dataFilter = SpatialQueryUtil.getFilterOpFromComparisonOp(comparisonOp, comparisonField,
                comparisonValue);
        Bson spatialFilter = SpatialQueryUtil.getSpatialOp(spatialOp, geometry);

        HashMap<String, String> results = new HashMap<>();

        if (dataFilter != null && spatialFilter != null) {
            FindIterable<Document> findIterable = collection.find(Filters.and(spatialFilter, dataFilter));
            MongoCursor<Document> cursor = findIterable.cursor();
            while (cursor.hasNext()) {
                log.info("hasNext()");
                String data = cursor.next().toJson();
                String gisJoin =
                        JsonParser.parseString(data).getAsJsonObject().getAsJsonPrimitive(Constants.GIS_JOIN).toString();

                // remove leading and trailing double quotes
                if (gisJoin.contains("\"")) {
                    gisJoin = gisJoin.replace("\"", "");
                }

                String geo = SpatialQueryUtil.getGeoFromGisJoin(resolution, gisJoin);

                results.put(data, geo);
            }
        } else {
            log.warn("FilterOp is null");
        }
        return results;
    }
}
