package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GeoIdResolver {
    private static final Logger log = LogManager.getLogger(GeoIdResolver.class);
    private static Connection dbConnection = null;
    private static String TABLE_NAME = "geoids";

    public static int resolveGeoId(double lat, double lng, String resolution) throws SQLException {
        log.info("Fetching " + resolution + " FIPS code for (" + lat + ", " + lng + ")");

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String query = "SELECT " + resolution + "_fips FROM " + TABLE_NAME + " latitude=? AND longitude=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setDouble(1, lat);
        statement.setDouble(2, lng);
        ResultSet resultSet = statement.executeQuery();

        int geoId = 0;
        while (resultSet.next()) {
            geoId = resultSet.getInt(resolution);
        }

        return geoId;
    }
}
