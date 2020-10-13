package org.sustain.querier;

import com.mongodb.client.FindIterable;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.BasicDBObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;

import org.json.JSONArray;
import org.json.JSONObject;

import org.sustain.JoinOperator;
import org.sustain.ComparisonOperator;
import org.sustain.CompoundResponse;
import org.sustain.CompoundRequest;
import org.sustain.Query;
import org.sustain.util.Constants;

import org.sustain.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Querier {
	private static final Logger log = LogManager.getLogger(Querier.class);

	public static void executeQuery(Query request, LinkedBlockingQueue<String> queue) {
        MongoDatabase db = DBConnection.getConnection(request.getHost(), Integer.toString(request.getPort()));
        MongoCollection<Document> collection = db.getCollection(request.getCollection());
        ArrayList<BasicDBObject> query = new ArrayList<BasicDBObject>();
        JSONArray parsedQuery = new JSONArray(request.getQuery());

        for (int i = 0; i < parsedQuery.length(); i++) {
            query.add(new BasicDBObject().parse(parsedQuery.getJSONObject(i).toString()));
         }
        AggregateIterable<Document> iterable = collection.aggregate(query);
        MongoCursor<Document> cursor = iterable.cursor();

        int count = 0;
        while (cursor.hasNext()) {
            Document next = cursor.next();
            queue.add(next.toJson());
            count++;
        }
        cursor.close();
        log.info("count: " + count);
    }
}