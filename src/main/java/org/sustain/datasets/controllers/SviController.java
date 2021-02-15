package org.sustain.datasets.controllers;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

/**
 * Controller to query Social Vulnerability Index data
 */
public class SviController {
    public static final Logger log = LogManager.getLogger(SviController.class);
    private static final MongoDatabase db = DBConnection.getConnection();

    public static String getSviByGisJoin(String gisJoin) {
        MongoCollection<Document> collection = db.getCollection("svi");

        FindIterable<Document> documents = collection.find(Filters.eq(Constants.GIS_JOIN, gisJoin));
        MongoCursor<Document> cursor = documents.cursor();
        if (cursor.hasNext()) {
            return cursor.next().toJson();
        } else {
            return null;
        }
    }
}
