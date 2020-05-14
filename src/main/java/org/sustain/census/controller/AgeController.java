package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.MedianAgeResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusFeatures.MEDIAN_AGE;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;

public class AgeController {
    private static final Logger log = LogManager.getLogger(AgeController.class);
    private static Connection dbConnection = null;

    public static MedianAgeResponse fetchMedianAge(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + MEDIAN_AGE + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "2011_" + resolutionKey + "_" + MEDIAN_AGE;

        String query = "SELECT " + MEDIAN_AGE + " FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        double medianage = 0;
        while (resultSet.next()) {
            medianage = resultSet.getInt(MEDIAN_AGE);
        }

        MedianAgeResponse response = MedianAgeResponse.newBuilder().setMedianAge(medianage).build();

        return response;
    }
}
