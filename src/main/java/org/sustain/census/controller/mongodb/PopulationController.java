package org.sustain.census.controller.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.Constants;
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

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonOp,
                                                            double comparisonValue) {
        return null;
    }
}
