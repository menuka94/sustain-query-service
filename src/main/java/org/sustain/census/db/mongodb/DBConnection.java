package org.sustain.census.db.mongodb;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.sustain.census.Constants;
import org.sustain.census.db.Util;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static String host;
    private static String port;
    private static String db;
    private static MongoClient mongoClient;

    private static void initDatabaseProperties() {
        host = Util.getProperty(Constants.DB.HOST);
        log.info("Host: " + host);
        port = Util.getProperty(Constants.DB.PORT);
        log.info("Port: " + port);
        db = Constants.DB.DB_NAME;
    }

    public static MongoDatabase getConnection() {
        if (mongoClient == null) {
            log.info("Creating new connection to MongoDB");
            initDatabaseProperties();
            mongoClient = new MongoClient(host, Integer.parseInt(port));
        }
        log.info("Connecting to MongoDB instance: {" + host + ":" + port + "}");
        return mongoClient.getDatabase(db);
    }

    public static void main(String[] args) {
        MongoDatabase connection = getConnection();
        MongoCollection<Document> tracts_geo = connection.getCollection("tracts_geo");
        MongoCollection<Document> states_geo = connection.getCollection("states_geo");
        //tracts_geo.find(Filters.eq("properties.GISJOIN", ""));
        //Document newYork = states_geo.find(Filters.eq("properties.STATENAM", "New York")).first();
        Document first = states_geo.find().first();
        JsonObject result = JsonParser.parseString(first.toJson()).getAsJsonObject();
        log.info(result.get("properties").getAsJsonObject().get("GISJOIN"));
        log.info(result.get("properties").getAsJsonObject().get("NAME10"));
    }
}
