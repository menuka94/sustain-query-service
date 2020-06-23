package org.sustain.census.controller.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.Constants;
import org.sustain.census.RaceResponse;
import org.sustain.census.db.mysql.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static org.sustain.census.Constants.CensusFeatures.RACE;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;

public class RaceController {
    private static final Logger log = LogManager.getLogger(RaceController.class);
    private static Connection dbConnection = null;

    private static final String WHITE = "white";
    private static final String BLACK = "black";
    private static final String AMERICAN_INDIAN_AND_ALASKA_NATIVE = "american_indian_and_alaska_native";
    private static final String ASIAN_AND_PACIFIC_ISLANDER_AND_OTHER = "asian_and_pacific_islander_and_other";

    public static RaceResponse fetchRace(String resolutionKey, int resolutionValue, String decade) throws SQLException {
        log.info("Fetching " + RACE + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "all_decades_" + resolutionKey + "_" + RACE;

        String query = "SELECT * FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, resolutionValue);

        log.info("Query: " + statement);

        ResultSet resultSet = statement.executeQuery();

        int white = 0;
        int black = 0;
        int americanIndianAndAlaskaNative = 0;
        int asianAndPacificIslanderAndOther = 0;
        while (resultSet.next()) {
            white = resultSet.getInt(decade + "_" + WHITE);
            black = resultSet.getInt(decade + "_" + BLACK);
            americanIndianAndAlaskaNative = resultSet.getInt(decade + "_" + AMERICAN_INDIAN_AND_ALASKA_NATIVE);
            asianAndPacificIslanderAndOther = resultSet.getInt(decade + "_" + ASIAN_AND_PACIFIC_ISLANDER_AND_OTHER);
        }

        RaceResponse response = RaceResponse.newBuilder()
                .setWhite(white)
                .setBlack(black)
                .setAmericanIndianAndAlaskaNative(americanIndianAndAlaskaNative)
                .setAsianAndPacificIslanderAndOther(asianAndPacificIslanderAndOther)
                .build();

        return response;
    }

    public static void main(String[] args) throws SQLException {
        int state_fips = 15;
        RaceResponse response1980 = fetchRace("state", state_fips, "1980");
        log.info("1980");
        log.info("White: " + response1980.getWhite());
        log.info("Black: " + response1980.getBlack());
        log.info("American Indian and Alaska Native: " + response1980.getAmericanIndianAndAlaskaNative());
        log.info("Asian and Pacific Islander and other: " + response1980.getAsianAndPacificIslanderAndOther());
        System.out.println();

        RaceResponse response1990 = fetchRace("state", state_fips, "1990");
        log.info("1990");
        log.info("White: " + response1990.getWhite());
        log.info("Black: " + response1990.getBlack());
        log.info("American Indian and Alaska Native: " + response1990.getAmericanIndianAndAlaskaNative());
        log.info("Asian and Pacific Islander and other: " + response1990.getAsianAndPacificIslanderAndOther());
        System.out.println();

        RaceResponse response2000 = fetchRace("state", state_fips, "2000");
        log.info("1990");
        log.info("White: " + response2000.getWhite());
        log.info("Black: " + response2000.getBlack());
        log.info("American Indian and Alaska Native: " + response2000.getAmericanIndianAndAlaskaNative());
        log.info("Asian and Pacific Islander and other: " + response2000.getAsianAndPacificIslanderAndOther());
        System.out.println();

        RaceResponse response2010 = fetchRace("state", state_fips, "2010");
        log.info("1990");
        log.info("White: " + response2010.getWhite());
        log.info("Black: " + response2010.getBlack());
        log.info("American Indian and Alaska Native: " + response2010.getAmericanIndianAndAlaskaNative());
        log.info("Asian and Pacific Islander and other: " + response2010.getAsianAndPacificIslanderAndOther());
        System.out.println();
    }

    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonField,
                                                            String comparisonOp, double comparisonValue) {
        return null;
    }
}
