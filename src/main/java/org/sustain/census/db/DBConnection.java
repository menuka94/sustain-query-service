package org.sustain.census.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static final String driverName = "com.mysql.cj.jdbc.Driver";

    private static final String username = "root";

    private static final String password = "toor";

    public static Connection getConnection(String dbName) {
        log.info("DBName: " + dbName);
        String url = "jdbc:mysql://localhost:3306/" + dbName + "?useSSL=false";
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

