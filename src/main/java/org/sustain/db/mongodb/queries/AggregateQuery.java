package org.sustain.db.mongodb.queries;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONArray;
import org.sustain.db.mongodb.DBConnection;

import java.util.ArrayList;

/**
 * Query object for MongoDB aggregate() queries
 */
public class AggregateQuery implements Query {

    @Override
    public AggregateIterable<Document> execute(String collection, String query) {

        // Connect to MongoDB
        MongoDatabase mongoDatabase = DBConnection.getConnection();
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        // Construct and build MongoDB query object
        ArrayList<BasicDBObject> bsonQuery = new ArrayList<>();
        JSONArray parsedQuery = new JSONArray(query);
        for (int i = 0; i < parsedQuery.length(); i++) {
            BasicDBObject queryComponent = BasicDBObject.parse(parsedQuery.getJSONObject(i).toString());
            bsonQuery.add(queryComponent);
        }

        // Submit query to MongoDB
        return mongoCollection.aggregate(bsonQuery);
    }


}
