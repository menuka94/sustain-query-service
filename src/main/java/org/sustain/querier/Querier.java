package org.sustain.querier;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.BasicDBObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;

import org.sustain.JoinOperator;
import org.sustain.ComparisonOperator;
import org.sustain.CompoundResponse;
import org.sustain.CompoundRequest;
import org.sustain.Query;

import org.sustain.db.mongodb.DBConnection;

import java.util.concurrent.LinkedBlockingQueue;

public class Querier {
	private static final Logger log = LogManager.getLogger(Querier.class);

	public static void executeQuery(Query request, LinkedBlockingQueue<String> queue) {
        MongoDatabase db = DBConnection.getConnection(request.getHost(), Integer.toString(request.getPort()));
        MongoCollection<Document> collection = db.getCollection(request.getCollection());
        BasicDBObject query = BasicDBObject.parse(request.getQuery());
        FindIterable<Document> iterable = collection.find(query);
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