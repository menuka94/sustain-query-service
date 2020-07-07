package org.sustain.census.controller.mongodb;

import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sustain.census.Constants;
import org.sustain.census.Predicate;
import org.sustain.census.db.mongodb.DBConnection;

import java.util.HashMap;

public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);

    public static String getPopulationResults(String resolution, String gisJoin) {
        log.info("getPopulationResults: " + "{resolution: " + resolution + ", GisJoin: " + gisJoin + "}");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + Constants.CensusFeatures.TOTAL_POPULATION);
        Document first = collection.find(Filters.eq(Constants.GIS_JOIN, gisJoin)).first();
        if (first != null) {
            return first.toJson();
        } else {
            log.warn("getPopulationResults(): empty results");
            return "";
        }
    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution,
                                                            Predicate.ComparisonOperator comparisonOp,
                                                            double comparisonValue) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + Constants.CensusFeatures.TOTAL_POPULATION);
        String comparisonField = decade + "_" + Constants.CensusFeatures.TOTAL_POPULATION;
        log.info("comparisonField: " + comparisonField);
        Bson filter = SpatialQueryUtil.getFilterOpFromComparisonOp(comparisonOp, comparisonField,
                comparisonValue);

        HashMap<String, String> results = new HashMap<>();

        if (filter != null) {
            FindIterable<Document> findIterable = collection.find(filter);
            MongoCursor<Document> cursor = findIterable.cursor();
            while (cursor.hasNext()) {
                String data = cursor.next().toJson();
                String gisJoin =
                        JsonParser.parseString(data).getAsJsonObject().getAsJsonPrimitive(Constants.GIS_JOIN).toString();
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
