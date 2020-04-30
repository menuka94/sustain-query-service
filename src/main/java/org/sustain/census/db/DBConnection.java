package org.sustain.census.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection {
    private static final Logger log = LogManager.getLogger(DBConnection.class);
    private static final String driverName = "com.mysql.cj.jdbc.Driver";

    private static String username;
    private static String password;
    private static String host;

    private static void initDatabaseProperties() {
        Properties properties = new Properties();
        String propFile = "config.properties";

        InputStream inputStream = DBConnection.class.getClassLoader().getResourceAsStream(propFile);

        if (inputStream != null) {
            try {
                properties.load(inputStream);

                username = properties.getProperty(Constants.DB.USERNAME);
                assert !"".equals(username);

                password = properties.getProperty(Constants.DB.PASSWORD);
                assert !"".equals(password);

                host = properties.getProperty(Constants.DB.HOST);
                assert !"".equals(host);

            } catch (IOException e) {
                e.printStackTrace();
                log.error("Error reading file: " + propFile);
                System.exit(0);
            }
        }
    }

    public static Connection getConnection(String dbName) {
        log.info("DBName: " + dbName);
        initDatabaseProperties();
        String url = "jdbc:mysql://" + host + ":3306/" + dbName + "?useSSL=false";
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

