package org.sustain.census.controller;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.util.Constants;
import org.sustain.db.mongodb.DBConnection;

import java.util.HashMap;

public class RaceController {
    private static final Logger log = LogManager.getLogger(RaceController.class);

    public static String getRace(String resolution, String gisJoin) {
        log.info("getRaceResults: " + "{resolution: " + resolution + ", GisJoin: " + gisJoin + "}");
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + Constants.CensusFeatures.RACE);
        Document first = collection.find(Filters.eq(Constants.GIS_JOIN, gisJoin)).first();
        if (first != null) {
            return first.toJson();
        } else {
            log.warn("getRace(): empty results");
            return "";
        }
    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonField,
                                                            String comparisonOp, double comparisonValue) {
        return null;
    }
}
