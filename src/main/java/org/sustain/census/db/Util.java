package org.sustain.census.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.mysql.DBConnection;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {
    private static final Logger log = LogManager.getLogger(Util.class);

    public static String getProperty(String property) {
        Properties properties = new Properties();
        String propFile = "config.properties";

        InputStream inputStream = DBConnection.class.getClassLoader().getResourceAsStream(propFile);

        if (inputStream != null) {
            try {
                properties.load(inputStream);
                String s = properties.getProperty(property);
                return s;

            } catch (IOException e) {
                e.printStackTrace();
                log.error("Error reading file: " + propFile);
                System.exit(0);
            }
        }
        return "";
    }
}
