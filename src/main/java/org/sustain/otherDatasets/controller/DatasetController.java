package org.sustain.otherDatasets.controller;

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
import org.sustain.census.Constants;
import org.sustain.census.DatasetRequest;
import org.sustain.census.SpatialOp;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.sustain.census.controller.SpatialQueryUtil.getGeometryFromGeoJson;

public class DatasetController {
    private static final Logger log = LogManager.getLogger(DatasetController.class);

    public static ArrayList<String> getData(DatasetRequest request) {
        String dataset = Constants.DATASETS.get(request.getDataset());
        Geometry geometry = getGeometryFromGeoJson(request.getRequestGeoJson());
        SpatialOp spatialOp = request.getSpatialOp();
        Map<String, String> requestParamsMap = request.getRequestParamsMap();

        log.info("dataData({dataset: " + dataset + ", spatialOp: " + spatialOp + "})");
        for (String key : requestParamsMap.keySet()) {
            log.info("{" + key + ": " + requestParamsMap.get(key) + "}");
        }

        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(dataset);

        List<Bson> filters = new ArrayList<>();
        filters.add(SpatialQueryUtil.getSpatialOp(spatialOp, geometry));
        for (String key : requestParamsMap.keySet()) {
            filters.add(Filters.eq(key, requestParamsMap.get(key)));
        }

        FindIterable<Document> documents = collection.find(Filters.and(filters));
        MongoCursor<Document> cursor = documents.cursor();

        ArrayList<String> results = new ArrayList<>();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            results.add(next.toJson());
        }

        return results;
    }
}
