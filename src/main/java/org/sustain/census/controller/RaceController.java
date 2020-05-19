package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.RaceResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.sustain.census.Constants.CensusFeatures.RACE;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;

public class RaceController {
    private static final Logger log = LogManager.getLogger(RaceController.class);
    private static Connection dbConnection = null;

    public static RaceResponse fetchRace(String resolutionKey, int resolutionValue) throws SQLException {
        log.info("Fetching " + RACE + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "2011_" + resolutionKey + "_" + RACE;

        String query = "SELECT * FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int whiteAlone = 0;
        int blackAlone = 0;
        int americanIndianAndAlaskaNative = 0;
        int asianAlone = 0;
        int nativeHawaiianAndPacificIslander = 0;
        int otherAlone = 0;
        int twoOrMoreRaces = 0;
        int latinoAlone = 0;
        int asiansAll = 0;
        int otherAll = 0;
        while (resultSet.next()) {
            whiteAlone = resultSet.getInt("white_alone");
            blackAlone = resultSet.getInt("black_alone");
            americanIndianAndAlaskaNative = resultSet.getInt("american_indian_and_alaska_native");
            asianAlone = resultSet.getInt("asian_alone");
            nativeHawaiianAndPacificIslander = resultSet.getInt("native_hawaiian_and_pacific_islander");
            otherAlone = resultSet.getInt("other_alone");
            twoOrMoreRaces = resultSet.getInt("two_or_more_races");
            latinoAlone = resultSet.getInt("latino_alone");
            asiansAll = resultSet.getInt("asian_all");
            otherAll = resultSet.getInt("other_all");
        }

        RaceResponse response = RaceResponse.newBuilder()
                .setWhiteAlone(whiteAlone)
                .setBlackAlone(blackAlone)
                .setAmericanIndianAndAlaskaNative(americanIndianAndAlaskaNative)
                .setAsianAlone(asianAlone)
                .setNativeHawaiianAndPacificIslander(nativeHawaiianAndPacificIslander)
                .setOtherAlone(otherAlone)
                .setTwoOrMoreRaces(twoOrMoreRaces)
                .setLatinoAlone(latinoAlone)
                .setAsiansAll(asiansAll)
                .setOtherAll(otherAll)
                .build();

        return response;
    }
}
