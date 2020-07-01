package org.sustain.census.controller.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.Constants;
import org.sustain.census.db.mongodb.DBConnection;

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
            return "";
        }
    }
}
