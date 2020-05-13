package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.DBConnection;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusResolutions.*;
import static org.sustain.census.Constants.DB.DB_NAME;

public class GeoIdResolver {
    private static final Logger log = LogManager.getLogger(GeoIdResolver.class);
    private static Connection dbConnection = null;
    private static String TABLE_NAME = "geoids";

    public static BigInteger resolveGeoId(double lat, double lng, String resolution) throws SQLException {
        log.info("Fetching " + resolution + " FIPS code for (" + lat + ", " + lng + ")");

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(DB_NAME);
        }

        boolean isTract = false;
        if (TRACT.equals(resolution)) {
            // tract_fips is not available in the table, but block_fips is
            // proceed to calculate tract_fips from block_fips
            resolution = BLOCK;
            isTract = true;
        }

        resolution += "_fips";
        log.info("Resolution: " + resolution);
        String query = "";
        if (isTract) {
            query = "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE " + LATITUDE + " like ? AND " + LONGITUDE + " like ?";
        } else {
            query = "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE " + LATITUDE + "=? AND " + LONGITUDE + "=?";
        }
        log.info("Query: " + query);

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setDouble(1, lat);
        statement.setDouble(2, lng);
        ResultSet resultSet = statement.executeQuery();

        BigInteger geoId = BigInteger.valueOf(0);
        while (resultSet.next()) {
            geoId = new BigInteger(resultSet.getObject(resolution).toString());
            log.info("Geo ID form resultSet: " + geoId);
            if (isTract) {
                String geoIdString = String.valueOf(geoId);
                // block_fips = 482012231001050
                // tract_fips = 48201223100
                geoId = new BigInteger(geoIdString.substring(0, geoIdString.length() - 4));
            }
        }

        return geoId;
    }

    public static void main(String[] args) throws SQLException {
//        double[] coordinates = new double[]{30.2, -88};
        double[] coordinates = new double[]{24.5, -82};
        BigInteger geoId = GeoIdResolver.resolveGeoId(coordinates[0], coordinates[1], COUNTY);
        log.info("GeoID: " + geoId);
    }
}
