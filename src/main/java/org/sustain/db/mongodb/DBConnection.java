package org.sustain.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.util.Constants;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static String host;
    private static Integer port;
    private static String db;
    private static MongoClient mongoClient;

    private static void initDatabaseProperties() {
        host = Constants.DB.HOST;
        log.info("Host: " + host);
        port = Constants.DB.PORT;
        log.info("Port: " + port);
        db = Constants.DB.NAME;
    }

    public static MongoDatabase getConnection() {
        if (mongoClient == null) {
            log.info("Creating new connection to MongoDB");
            initDatabaseProperties();
            mongoClient = new MongoClient(host, port);
        }
        log.debug("Connecting to MongoDB instance: {" + host + ":" + port + "}");
        return mongoClient.getDatabase(db);
    }

    public static MongoDatabase getConnection(String host, String port) {
        if (mongoClient == null) {
            log.info("Creating new connection to MongoDB");
            initDatabaseProperties();
            mongoClient = new MongoClient(host, Integer.parseInt(port));
        }
        log.debug("Connecting to MongoDB instance: {" + host + ":" + port + "}");
        return mongoClient.getDatabase(db);
    }
}
