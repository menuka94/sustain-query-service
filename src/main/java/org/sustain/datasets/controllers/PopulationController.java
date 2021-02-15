package org.sustain.datasets.controllers;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sustain.CensusFeature;
import org.sustain.CensusResolution;
import org.sustain.Decade;
import org.sustain.Predicate;
import org.sustain.SpatialOp;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);

    public static void getTotalPopulationResults(String resolution, ArrayList<String> geoJsonList,
                                                 LinkedBlockingQueue<String> queue) {
        getPopulationResults(resolution, geoJsonList, Constants.CensusFeatures.TOTAL_POPULATION, queue);
    }

    public static void getPopulationByAgeResults(String resolution, ArrayList<String> geoJsonList,
                                                 LinkedBlockingQueue<String> queue) {
        getPopulationResults(resolution, geoJsonList, Constants.CensusFeatures.POPULATION_BY_AGE, queue);
    }

    // common method used for fetching both 'total_population' and 'population_by_age'
    private static void getPopulationResults(String resolution, ArrayList<String> geoJsonList,
                                             String collectionName,
                                             LinkedBlockingQueue<String> queue) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection =
                db.getCollection(resolution + "_" + collectionName);
        FindIterable<Document> iterable = collection.find(Filters.in("GISJOIN", geoJsonList));
        MongoCursor<Document> cursor = iterable.cursor();

        int count = 0;
        while (cursor.hasNext()) {
            Document next = cursor.next();
            queue.add(next.toJson());
            count++;
        }
        log.info("count: " + count);
    }

}
