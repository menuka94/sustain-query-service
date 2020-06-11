package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.MedianHouseholdIncomeResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.sustain.census.Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;

public class IncomeController {
    private static final Logger log = LogManager.getLogger(IncomeController.class);
    private static Connection dbConnection = null;

    /**
     * @param resolutionKey   : ex:- "state", "county", or "tract"
     * @param resolutionValue : ex:- state_fips, county_fips, or tract_fips
     */
    public static MedianHouseholdIncomeResponse fetchMedianHouseholdIncome(String resolutionKey, String resolutionValue,
                                                                           String decade) throws SQLException {
        log.info("Fetching " + MEDIAN_HOUSEHOLD_INCOME + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "all_decades_" + resolutionKey + "_" + MEDIAN_HOUSEHOLD_INCOME;
        final String COLUMN = decade + "_" + MEDIAN_HOUSEHOLD_INCOME;

        String query = "SELECT " + COLUMN + " FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setString(1, resolutionValue);

        log.info("Query: " + statement);

        ResultSet resultSet = statement.executeQuery();

        int income = 0;
        while (resultSet.next()) {
            income = resultSet.getInt(COLUMN);
        }

        return MedianHouseholdIncomeResponse.newBuilder().setMedianHouseholdIncome(income).build();
    }

    public static MedianHouseholdIncomeResponse getAveragedMedianHouseholdIncome(String resolution,
                                                                                 ArrayList<String> geoIds,
                                                                                 String decade) throws SQLException {
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "all_decades_" + resolution + "_" + MEDIAN_HOUSEHOLD_INCOME;
        final String COLUMN = decade + "_" + MEDIAN_HOUSEHOLD_INCOME;

        String joinedGeoIds = String.join(", ", geoIds);
        String query =
                "SELECT " + COLUMN + " FROM " + tableName + " WHERE " + GEO_ID + " IN (" + joinedGeoIds + ");";

        log.info("Query: " + query);
        PreparedStatement statement = dbConnection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        int incomeSum = 0;
        int incomeCount = 0;
        while (resultSet.next()) {
            incomeSum += resultSet.getInt(COLUMN);
            incomeCount++;
        }

        log.debug("incomeSum: " + incomeSum);
        log.debug("incomeCount: " + incomeCount);
        double averageIncome = (double) incomeSum / incomeCount;

        return MedianHouseholdIncomeResponse.newBuilder().setMedianHouseholdIncome(averageIncome).build();
    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonOp,
                                                            double comparisonValue) throws SQLException {
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        final String TABLE_NAME = "all_decades_" + resolution + "_" + MEDIAN_HOUSEHOLD_INCOME;
        final String COLUMN = decade + "_" + MEDIAN_HOUSEHOLD_INCOME;

        // no risk of SQL Injection since variables 'resolution' and 'comparisonOp' are validated through the
        // CensusServer.
        String query =
                "SELECT geoid," + resolution + " FROM " + TABLE_NAME + " WHERE " + COLUMN + " " + comparisonOp + " ?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setString(1, Double.toString(comparisonValue));

        log.info("Query: " + statement);

        ResultSet resultSet = statement.executeQuery();

        HashMap<String, String> results = new HashMap<>();
        while (resultSet.next()) {
            results.put(
                    resultSet.getString(GEO_ID),
                    resultSet.getString(resolution)
            );
        }

        return results;
    }


    public static void main(String[] args) throws SQLException {
        String state_fips = "10";
        MedianHouseholdIncomeResponse income2010 = fetchMedianHouseholdIncome("state", state_fips, "2010");
        MedianHouseholdIncomeResponse income2000 = fetchMedianHouseholdIncome("state", state_fips, "2000");
        MedianHouseholdIncomeResponse income1990 = fetchMedianHouseholdIncome("state", state_fips, "1990");
        MedianHouseholdIncomeResponse income1980 = fetchMedianHouseholdIncome("state", state_fips, "1980");

        log.info("Income 2010: " + income2010.getMedianHouseholdIncome());
        log.info("Income 2000: " + income2000.getMedianHouseholdIncome());
        log.info("Income 1990 " + income1990.getMedianHouseholdIncome());
        log.info("Income 1980: " + income1980.getMedianHouseholdIncome());

        //HashMap<String, String> results = fetchTargetedInfo("2010", "state", ">=", 50000);
        //for (String geoid : results.keySet()) {
        //    log.info(geoid + ": " + results.get(geoid));
        //}
    }

}

