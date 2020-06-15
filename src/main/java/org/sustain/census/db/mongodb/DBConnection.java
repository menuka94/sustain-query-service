package org.sustain.census.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.sustain.census.Constants;

public class DBConnection {
    public static MongoDatabase getConnection() {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        return mongoClient.getDatabase(Constants.DB.DB_NAME);
    }
}
