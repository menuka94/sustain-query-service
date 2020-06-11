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
import java.util.ArrayList;

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
        log.debug("Resolution: " + resolution);
        String query =
                "SELECT " + resolution + " FROM " + TABLE_NAME + " WHERE " + LATITUDE + " LIKE ? AND " + LONGITUDE +
                        " LIKE ?";

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

    /**
     * Get all GeoIDs that fall within a given bounding box (x1, x2, y1, y2)
     *
     * @param resolution - census resolution
     */
    public static ArrayList<String> getGeoIdsInBoundingBox(double x1, double x2, double y1, double y2,
                                                           String resolution) throws SQLException {
        ArrayList<String> geoIds = new ArrayList<>();
        x1 = Double.parseDouble(formatter.format(x1)) * 10;
        x2 = Double.parseDouble(formatter.format(x2)) * 10;
        y1 = Double.parseDouble(formatter.format(y1)) * 10;
        y2 = Double.parseDouble(formatter.format(y2)) * 10;

        int intX1 = (int) x1;
        int intX2 = (int) x2;
        int intY1 = (int) y1;
        int intY2 = (int) y2;
        for (int i = intX1; i <= intX2; i += 1) {
            for (int j = intY1; j <= intY2; j += 1) {
                String geoId = resolveGeoId(0.1 * i, 0.1 * j, resolution);
                if (!"".equals(geoId)) {
                    log.debug("Successfully resolved GeoID: " + geoId);
                    geoIds.add(geoId);
                }
            }
        }
        return geoIds;
    }
}
