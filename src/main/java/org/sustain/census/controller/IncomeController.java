package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusFeatures.MEDIAN_HOUSEHOLD_INCOME;

public class IncomeController {
    private static final Logger log = LogManager.getLogger(IncomeController.class);
    private static Connection dbConnection = null;

    /**
     * @param resolutionKey   : ex:- "state", or "county"
     * @param resolutionValue : ex:- stateID, or countyID
     * @return totalPopulation for the area specified by resolutionValue
     */
    public static int fetchMedianHouseholdIncome(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + MEDIAN_HOUSEHOLD_INCOME + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        // state_total_population
        String tableName = "2011_" + resolutionKey + "_" + MEDIAN_HOUSEHOLD_INCOME;

        String query = "SELECT income FROM " + tableName + " WHERE geoid=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int total = 0;
        while (resultSet.next()) {
            total = resultSet.getInt("income");
        }

        return total;
    }

    public static void main(String[] args) throws SQLException {
        int stateId = 05;
        int income = fetchMedianHouseholdIncome(Constants.CensusResolutions.STATE, stateId);
        log.info("Median Household Income for state " + stateId + " is $" + income + "/year");
    }
}
