package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.PopulationByAgeResponse;
import org.sustain.census.TotalPopulationResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusFeatures.POPULATION_BY_AGE;
import static org.sustain.census.Constants.CensusFeatures.TOTAL_POPULATION;

public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);
    private static Connection dbConnection = null;

    /**
     * @param resolutionKey   : ex:- "state", or "county"
     * @param resolutionValue : ex:- stateID, or countyID
     * @return totalPopulation for the area specified by resolutionValue
     */
    public static TotalPopulationResponse fetchTotalPopulation(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + TOTAL_POPULATION + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "2011_" + resolutionKey + "_" + TOTAL_POPULATION;

        String query = "SELECT total FROM " + tableName + " WHERE " + Constants.CensusResolutions.GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int total = 0;
        while (resultSet.next()) {
            total = resultSet.getInt("total");
        }

        TotalPopulationResponse response = TotalPopulationResponse.newBuilder().setPopulation(total).build();
        return response;
    }

    public static PopulationByAgeResponse fetchPopulationByAge(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + POPULATION_BY_AGE + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        // state_total_population
        String tableName = resolutionKey + "_" + POPULATION_BY_AGE;

        String query = "SELECT * FROM " + tableName + " WHERE " + Constants.CensusResolutions.GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int maleTotal = 0;
        int male5To9 = 0;
        int male10To14 = 0;
        while (resultSet.next()) {
            maleTotal = resultSet.getInt("male_total");
            male5To9 = resultSet.getInt("male_5_to_9");
            male10To14 = resultSet.getInt("male_10_to_14");
        }

        PopulationByAgeResponse response = PopulationByAgeResponse.newBuilder()
                .setMaleTotal(maleTotal).setMale5To9(male5To9).setMale10To14(male10To14).build();

        return response;
    }

    public static void main(String[] args) throws SQLException {
        int stateCode = 50;
        TotalPopulationResponse totalPopulation = fetchTotalPopulation("state", stateCode);
        log.info("Total Population for state " + stateCode + ": " + totalPopulation.getPopulation());
    }
}
