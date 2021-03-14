package org.sustain.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.util.Constants;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);

    private static MongoClient mongoClient;

    public static MongoDatabase getConnection() {
        return getConnection(Constants.DB.HOST, Constants.DB.PORT);
    }

    public static MongoDatabase getConnection(String host, Integer port) {
        if (mongoClient == null) {
            log.info("Creating new connection to MongoDB");
            mongoClient = new MongoClient(host, port);
        }
        log.info("Connecting to MongoDB instance: mongodb://{}:{}", host, port);
        return mongoClient.getDatabase(Constants.DB.NAME);
    }
}
