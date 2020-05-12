package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.db.DBConnection;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GeoIdResolver {
    private static final Logger log = LogManager.getLogger(GeoIdResolver.class);
    private static Connection dbConnection = null;
    private static String TABLE_NAME = "geoids";

    public static BigInteger resolveGeoId(double lat, double lng, String resolution) throws SQLException {
        log.info("Fetching " + resolution + " FIPS code for (" + lat + ", " + lng + ")");

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        if (Constants.CensusResolutions.TRACT.equals(resolution)) {
            // tract_fips is not available in the table, but block_fips is
            // proceed to calculate tract_fips from block_fips
            resolution = Constants.CensusResolutions.BLOCK;
        }

        resolution += "_fips";
        log.info("Resolution: " + resolution);
        String query = "";
        if (Constants.CensusResolutions.TRACT.equals(resolution)) {
            query = "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE latitude like ? AND longitude like ?";
        } else {
            query = "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE latitude=? AND longitude=?";
        }
        log.info("Query: " + query);

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setDouble(1, lat);
        statement.setDouble(2, lng);
        ResultSet resultSet = statement.executeQuery();

        BigInteger geoId = BigInteger.valueOf(0);
        while (resultSet.next()) {
            geoId = (BigInteger) resultSet.getObject(resolution);
            log.info("Geo ID form resultSet: " + geoId);
            if (Constants.CensusResolutions.TRACT.equals(resolution)) {
                String geoIdString = String.valueOf(geoId);
                // block_fips = 482012231001050
                // tract_fips = 48201223100
                geoId = new BigInteger(geoIdString.substring(0, geoIdString.length() - 4));
            }
        }

        return geoId;
    }

    public static void main(String[] args) throws SQLException {
        BigInteger geoId = GeoIdResolver.resolveGeoId(30.2, -88.0, Constants.CensusResolutions.TRACT);
        log.info("GeoID: " + geoId);
    }
}
