package org.sustain.mongodb.queries;

import org.bson.Document;

/**
 * General interface for all MongoDB queries
 */
public interface Query {

    /**
     * Executes a query on a MongoDB collection.
     * @param collection The MongoDB collection to execute the query on
     * @param query The JSON String query to be executed
     * @return An Iterable of resulting BSON Documents
     */
    public Iterable<Document> execute(String collection, String query);

}
