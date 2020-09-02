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
import org.sustain.DatasetRequest;
import org.sustain.SpatialOp;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.sustain.census.controller.SpatialQueryUtil.getGeometryFromGeoJson;

public class DatasetController {
    private static final Logger log = LogManager.getLogger(DatasetController.class);

    public static void getData(DatasetRequest request, LinkedBlockingQueue<String> queue) {
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

        while (cursor.hasNext()) {
            Document next = cursor.next();
            queue.add(next.toJson());
        }
    }
}
