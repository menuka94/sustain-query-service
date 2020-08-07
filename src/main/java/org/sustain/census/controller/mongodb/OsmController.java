package org.sustain.census.controller.mongodb;

import com.mongodb.client.AggregateIterable;
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
import org.sustain.census.OsmRequest;
import org.sustain.census.SpatialOp;
import org.sustain.census.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.sustain.census.controller.mongodb.SpatialQueryUtil.getGeometryFromGeoJson;

public class OsmController {
    private static final Logger log = LogManager.getLogger(OsmController.class);

    public static ArrayList<String> getOsmData(OsmRequest request) {
        String dataset = Constants.OSM_DATASETS.get(request.getDataset());
        Geometry geometry = getGeometryFromGeoJson(request.getRequestGeoJson());
        SpatialOp spatialOp = request.getSpatialOp();
        Map<String, String> requestParamsMap = request.getRequestParamsMap();

        log.info("getOsmData({dataset: " + dataset + ", spatialOp: " + spatialOp + "})");
        for (String key : requestParamsMap.keySet()) {
            log.info("{" + key + ": " + requestParamsMap.get(key) + "}");
        }

        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(dataset);

        List<Bson> orFilters = new ArrayList<>();
        Bson spatialFilter = SpatialQueryUtil.getSpatialOp(spatialOp, geometry);
        for (String key : requestParamsMap.keySet()) {
            orFilters.add(Filters.eq(key, requestParamsMap.get(key)));
        }

        AggregateIterable<Document> aggregate = collection.aggregate(Arrays.asList(
                Filters.and(spatialFilter),
                Filters.or(orFilters)
        ));

        MongoCursor<Document> cursor = aggregate.cursor();

        ArrayList<String> results = new ArrayList<>();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            results.add(next.toJson());
        }

        return results;
    }
}
