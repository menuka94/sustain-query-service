package org.sustain.mongodb.queries;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.sustain.mongodb.DBConnection;

/**
 * Query object for MongoDB find() queries
 */
public class FindQuery implements Query {

    @Override
    public FindIterable<Document> execute(String collection, String query) {
        // Connect to MongoDB
        MongoDatabase mongoDatabase = DBConnection.getConnection();
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        // Construct and build MongoDB query object
        BasicDBObject filter = BasicDBObject.parse(query);

        // Submit query to MongoDB
        return mongoCollection.find(filter);
    }
}
