package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);
    private static final String TOTAL_POPULATION = "total_population";
    private static final String POPULATION_BY_AGE = "population_by_age";
    private static Connection dbConnection = null;

    /**
     * @param resolutionKey   : ex:- "state", or "county"
     * @param resolutionValue : ex:- stateID, or countyID
     * @return totalPopulation for the area specified by resolutionValue
     */
    public static int fetchTotalPopulation(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + TOTAL_POPULATION + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        // state_total_population
        String tableName = "2014_" + resolutionKey + "_" + TOTAL_POPULATION;

        String query = "SELECT total FROM " + tableName + " WHERE geoid=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int total = 0;
        while (resultSet.next()) {
            total = resultSet.getInt("total");
        }

        return total;
    }

    public static int fetchPopulationByAge(String resolutionKey, int resolutionValue, String age) throws SQLException {
        log.info("Fetching " + POPULATION_BY_AGE + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        // state_total_population
        String tableName = resolutionKey + "_" + POPULATION_BY_AGE;

        String query = "SELECT total FROM " + tableName + " WHERE geoid=? AND age=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        statement.setString(2, age);
        ResultSet resultSet = statement.executeQuery();

        int valueByAge = 0;
        while (resultSet.next()) {
            valueByAge = resultSet.getInt(age);
        }

        return valueByAge;
    }

    public static void main(String[] args) throws SQLException {
        int stateCode = 50;
        int totalPopulation = fetchTotalPopulation("state", stateCode);
        log.info("Total Population for state " + stateCode + ": " + totalPopulation);
    }
}
