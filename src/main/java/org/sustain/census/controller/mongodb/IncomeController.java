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

public class IncomeController {
    private static final Logger log = LogManager.getLogger(IncomeController.class);

    public static String getMedianHouseholdIncome(String resolution, String gisJoin) {
        log.info("getMedianHouseholdIncomeResults: " + "{resolution: " + resolution + ", GisJoin: " + gisJoin + "}");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME);
        Document first = collection.find(Filters.eq(Constants.GIS_JOIN, gisJoin)).first();
        if (first != null) {
            return first.toJson();
        } else {
            log.warn("getMedianHouseholdIncome(): empty results");
            return "";
        }
    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonOp, double comparisonValue) {
        return null;
    }
}
