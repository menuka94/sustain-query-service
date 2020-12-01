package org.sustain.db.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.util.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static final String driverName = "com.mysql.cj.jdbc.Driver";

    private static String username;
    private static String password;
    private static String host;

    private static void initDatabaseProperties() {
        username = Constants.DB.USERNAME;
        password = Constants.DB.PASSWORD;
        host     = Constants.DB.HOST;
    }

    public static Connection getConnection(String dbName) {
        log.debug("DBName: " + dbName);
        initDatabaseProperties();
        String url = "jdbc:mysql://" + host + ":3306/" + dbName + "?useSSL=false&" +
                "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        log.debug("Connection URL: " + url);
        Connection con = null;
        try {
            Class.forName(driverName);
            try {
                con = DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                e.printStackTrace();
                log.info("Failed to create the database connection.");
                log.info("Exiting ...");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.info("Driver not found.");
            log.info("Exiting ...");
            System.exit(1);
        }
        return con;
    }
}

