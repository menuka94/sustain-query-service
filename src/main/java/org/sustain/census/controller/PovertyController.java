package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.PovertyResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusFeatures.POVERTY;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;

public class PovertyController {
    private static final Logger log = LogManager.getLogger(PovertyController.class);
    private static Connection dbConnection = null;

    public static PovertyResponse fetchPovertyData(String resolutionKey, String resolutionValue) throws SQLException {
        log.info("Fetching " + POVERTY + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "2011_" + resolutionKey + "_" + POVERTY;

        String query = "SELECT * FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, Integer.parseInt(resolutionValue));
        ResultSet resultSet = statement.executeQuery();

        int incomeBelowPovertyLevel = 0;
        int incomeAtOrAbovePovertyLevel = 0;
        while (resultSet.next()) {
            incomeBelowPovertyLevel = resultSet.getInt("income_below_poverty_level");
            incomeAtOrAbovePovertyLevel = resultSet.getInt("income_at_or_above_poverty_level");
        }

        PovertyResponse response = PovertyResponse.newBuilder()
                .setIncomeBelowPovertyLevel(incomeBelowPovertyLevel)
                .setIncomeAtOrAbovePovertyLevel(incomeAtOrAbovePovertyLevel)
                .build();

        return response;
    }
}
