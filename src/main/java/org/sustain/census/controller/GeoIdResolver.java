package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import static org.sustain.census.Constants.CensusResolutions.COUNTY;
import static org.sustain.census.Constants.CensusResolutions.LATITUDE;
import static org.sustain.census.Constants.CensusResolutions.LONGITUDE;
import static org.sustain.census.Constants.CensusResolutions.STATE;
import static org.sustain.census.Constants.CensusResolutions.TRACT;
import static org.sustain.census.Constants.DB.DB_NAME;

public class GeoIdResolver {
    private static final Logger log = LogManager.getLogger(GeoIdResolver.class);
    private static Connection dbConnection = null;
    private static final String TABLE_NAME = "geoids";
    private static final NumberFormat formatter = new DecimalFormat("#0.0");

    public static String resolveGeoId(double lat, double lng, String resolution) throws SQLException {
        log.info("Fetching " + resolution + " FIPS code for (" + lat + ", " + lng + ")");

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(DB_NAME);
        }

        resolution += "_fips";
        log.info("Resolution: " + resolution);
        String query = "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE " + LATITUDE + "=? AND " + LONGITUDE +
                "=?";

        lat = Double.parseDouble(formatter.format(lat));
        lng = Double.parseDouble(formatter.format(lng));

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setDouble(1, lat);
        statement.setDouble(2, lng);
        log.info("Query: " + statement);
        ResultSet resultSet = statement.executeQuery();

        String geoId = "";
        while (resultSet.next()) {
            geoId = resultSet.getString(resolution);
            log.info("Geo ID form resultSet: " + geoId);
        }

        return geoId;
    }

    public static void main(String[] args) throws SQLException {
        //double[] coordinates = new double[]{30.2, -88};
        //double[] coordinates = new double[]{40.5, -105};
        double[] coordinates = new double[]{24.5, -82.0};
        String stateFips = GeoIdResolver.resolveGeoId(coordinates[0], coordinates[1], STATE);
        String countyFips = GeoIdResolver.resolveGeoId(coordinates[0], coordinates[1], COUNTY);
        String tractFips = GeoIdResolver.resolveGeoId(coordinates[0], coordinates[1], TRACT);
        log.info("StateFIPS: " + stateFips);
        log.info("CountyFIPS: " + countyFips);
        log.info("TractFIPS: " + tractFips);
    }
}
