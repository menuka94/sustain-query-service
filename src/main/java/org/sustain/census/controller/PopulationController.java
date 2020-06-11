package org.sustain.census.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.AgeCategories;
import org.sustain.census.Constants;
import org.sustain.census.FemaleAgeCategories;
import org.sustain.census.MaleAgeCategories;
import org.sustain.census.PopulationByAgeResponse;
import org.sustain.census.TotalPopulationResponse;
import org.sustain.census.db.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.sustain.census.Constants.CensusFeatures.POPULATION;
import static org.sustain.census.Constants.CensusFeatures.POPULATION_BY_AGE;
import static org.sustain.census.Constants.CensusResolutions.GEO_ID;


public class PopulationController {
    private static final Logger log = LogManager.getLogger(PopulationController.class);
    private static Connection dbConnection = null;

    /**
     * @param resolutionKey   : ex:- "state", or "county"
     * @param resolutionValue : ex:- stateID, or countyID
     */
    public static TotalPopulationResponse fetchTotalPopulation(String resolutionKey, String resolutionValue,
                                                               String decade) throws SQLException {
        log.info("Fetching " + POPULATION + " for " + resolutionKey + ": " + resolutionValue);

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "all_decades_" + resolutionKey + "_" + POPULATION;
        final String COLUMN = decade + "_" + POPULATION;

        String query = "SELECT " + COLUMN + " FROM " + tableName + " WHERE " + GEO_ID + "=?";
        PreparedStatement statement = dbConnection.prepareStatement(query);

        statement.setString(1, resolutionValue);
        log.info("Query: " + statement);

        ResultSet resultSet = statement.executeQuery();

        int population = 0;
        while (resultSet.next()) {
            population = resultSet.getInt(COLUMN);
        }

        return TotalPopulationResponse.newBuilder().setPopulation(population).build();
    }

    public static TotalPopulationResponse getAveragedPopulation(String resolution, ArrayList<String> geoIds,
                                                                String decade) throws SQLException {
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        String tableName = "all_decades_" + resolution + "_" + POPULATION;
        final String COLUMN = decade + "_" + POPULATION;

        String joinedGeoIds = String.join(", ", geoIds);
        String query =
                "SELECT " + COLUMN + " FROM " + tableName + " WHERE " + GEO_ID + " IN (" + joinedGeoIds + ");";

        log.info("Query: " + query);
        PreparedStatement statement = dbConnection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        int populationSum = 0;
        int populationCount = 0;
        while (resultSet.next()) {
            populationSum += resultSet.getInt(COLUMN);
            populationCount++;
        }

        log.debug("populationSum: " + populationSum);
        log.debug("populationCount: " + populationCount);
        double averageIncome = (double) populationSum / populationCount;

        return TotalPopulationResponse.newBuilder().setPopulation(averageIncome).build();
    }

    public static PopulationByAgeResponse fetchPopulationByAge(String resolutionKey, String resolutionValue) throws SQLException {
        log.info("Fetching " + POPULATION_BY_AGE + " for " + resolutionKey + ": " + resolutionValue);
        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }
        // state_Total_population
        String tableName = "2011_" + resolutionKey + "_" + POPULATION_BY_AGE;

        String query = "SELECT * FROM " + tableName + " WHERE " + GEO_ID + "=?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setString(1, resolutionValue);
        ResultSet resultSet = statement.executeQuery();

        int maleTotal = 0, femaleTotal = 0;
        int maleUnder5 = 0, femaleUnder5 = 0;
        int male5To9 = 0, female5To9 = 0;
        int male10To14 = 0, female10To14 = 0;
        int male15To17 = 0, female15To17 = 0;
        int male18To19 = 0, female18To19 = 0;
        int male20 = 0, female20 = 0;
        int male21 = 0, female21 = 0;
        int male22To24 = 0, female22To24 = 0;
        int male25To29 = 0, female25To29 = 0;
        int male30To34 = 0, female30To34 = 0;
        int male35To39 = 0, female35To39 = 0;
        int male40To44 = 0, female40To44 = 0;
        int male45To49 = 0, female45To49 = 0;
        int male50To54 = 0, female50To54 = 0;
        int male55To59 = 0, female55To59 = 0;
        int male60To61 = 0, female60To61 = 0;
        int male62To64 = 0, female62To64 = 0;
        int male65To66 = 0, female65To66 = 0;
        int male67To69 = 0, female67To69 = 0;
        int male70To74 = 0, female70To74 = 0;
        int male75To79 = 0, female75To79 = 0;
        int male80To84 = 0, female80To84 = 0;
        int male85andOver = 0, female85andOver = 0;

        while (resultSet.next()) {
            maleTotal = resultSet.getInt("male_Total");
            maleUnder5 = resultSet.getInt("male_under_5");
            male5To9 = resultSet.getInt("male_5_To_9");
            male10To14 = resultSet.getInt("male_10_To_14");
            male15To17 = resultSet.getInt("male_15_To_17");
            male18To19 = resultSet.getInt("male_18_To_19");
            male20 = resultSet.getInt("male_20");
            male21 = resultSet.getInt("male_21");
            male22To24 = resultSet.getInt("male_22_To_24");
            male25To29 = resultSet.getInt("male_25_To_29");
            male30To34 = resultSet.getInt("male_30_To_34");
            male35To39 = resultSet.getInt("male_35_To_39");
            male40To44 = resultSet.getInt("male_40_To_44");
            male45To49 = resultSet.getInt("male_45_To_49");
            male50To54 = resultSet.getInt("male_50_To_54");
            male55To59 = resultSet.getInt("male_55_To_59");
            male60To61 = resultSet.getInt("male_60_To_61");
            male62To64 = resultSet.getInt("male_62_To_64");
            male65To66 = resultSet.getInt("male_65_To_66");
            male67To69 = resultSet.getInt("male_67_To_69");
            male70To74 = resultSet.getInt("male_70_To_74");
            male75To79 = resultSet.getInt("male_75_To_79");
            male80To84 = resultSet.getInt("male_80_To_84");
            male85andOver = resultSet.getInt("male_85_and_over");

            femaleTotal = resultSet.getInt("female_Total");
            femaleUnder5 = resultSet.getInt("female_under_5");
            female5To9 = resultSet.getInt("female_5_To_9");
            female10To14 = resultSet.getInt("female_10_To_14");
            female15To17 = resultSet.getInt("female_15_To_17");
            female18To19 = resultSet.getInt("female_18_To_19");
            female20 = resultSet.getInt("female_20");
            female21 = resultSet.getInt("female_21");
            female22To24 = resultSet.getInt("female_22_To_24");
            female25To29 = resultSet.getInt("female_25_To_29");
            female30To34 = resultSet.getInt("female_30_To_34");
            female35To39 = resultSet.getInt("female_35_To_39");
            female40To44 = resultSet.getInt("female_40_To_44");
            female45To49 = resultSet.getInt("female_45_To_49");
            female50To54 = resultSet.getInt("female_50_To_54");
            female55To59 = resultSet.getInt("female_55_To_59");
            female60To61 = resultSet.getInt("female_60_To_61");
            female62To64 = resultSet.getInt("female_62_To_64");
            female65To66 = resultSet.getInt("female_65_To_66");
            female67To69 = resultSet.getInt("female_67_To_69");
            female70To74 = resultSet.getInt("female_70_To_74");
            female75To79 = resultSet.getInt("female_75_To_79");
            female80To84 = resultSet.getInt("female_80_To_84");
            female85andOver = resultSet.getInt("female_85_and_over");
        }

        PopulationByAgeResponse response = PopulationByAgeResponse.newBuilder()
                .setMaleAgeCategories(MaleAgeCategories.newBuilder().setAgeCategories(AgeCategories.newBuilder()
                                .setTotal(maleTotal)
                                .setUnder5(maleUnder5)
                                .set5To9(male5To9)
                                .set10To14(male10To14)
                                .set15To17(male15To17)
                                .set18To19(male18To19)
                                .set20(male20)
                                .set21(male21)
                                .set22To24(male22To24)
                                .set25To29(male25To29)
                                .set30To34(male30To34)
                                .set35To39(male35To39)
                                .set40To44(male40To44)
                                .set45To49(male45To49)
                                .set50To54(male50To54)
                                .set55To59(male55To59)
                                .set60To61(male60To61)
                                .set62To64(male62To64)
                                .set65To66(male65To66)
                                .set67To69(male67To69)
                                .set70To74(male70To74)
                                .set75To79(male75To79)
                                .set80To84(male80To84)
                                .set85AndOver(male85andOver)
                        )
                )
                .setFemaleAgeCategories(FemaleAgeCategories.newBuilder().setAgeCategories(AgeCategories.newBuilder()
                                .setTotal(femaleTotal)
                                .setUnder5(femaleUnder5)
                                .set5To9(female5To9)
                                .set10To14(female10To14)
                                .set15To17(female15To17)
                                .set18To19(female18To19)
                                .set20(female20)
                                .set21(female21)
                                .set22To24(female22To24)
                                .set25To29(female25To29)
                                .set30To34(female30To34)
                                .set35To39(female35To39)
                                .set40To44(female40To44)
                                .set45To49(female45To49)
                                .set50To54(female50To54)
                                .set55To59(female55To59)
                                .set60To61(female60To61)
                                .set62To64(female62To64)
                                .set65To66(female65To66)
                                .set67To69(female67To69)
                                .set70To74(female70To74)
                                .set75To79(female75To79)
                                .set80To84(female80To84)
                                .set85AndOver(female85andOver)
                        )
                ).build();

        return response;
    }


    public static HashMap<String, String> fetchTargetedInfo(String decade, String resolution, String comparisonOp,
                                                            double comparisonValue) throws SQLException {

        if (dbConnection == null) {
            dbConnection = DBConnection.getConnection(Constants.DB.DB_NAME);
        }

        final String TABLE_NAME = "all_decades_" + resolution + "_" + POPULATION;
        final String COLUMN = decade + "_" + POPULATION;

        // no risk of SQL Injection since variables 'resolution' and 'comparisonOp' are validated through the
        // CensusServer.
        String query = "SELECT " + GEO_ID + "," + resolution + " FROM " + TABLE_NAME + " WHERE " + COLUMN + " " +
                comparisonOp + " ?";

        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setString(1, Double.toString(comparisonValue));

        log.info("Query: " + statement);

        ResultSet resultSet = statement.executeQuery();

        HashMap<String, String> results = new HashMap<>();
        while (resultSet.next()) {
            results.put(
                    Long.toString(resultSet.getLong(GEO_ID)),
                    resultSet.getString(resolution)
            );
        }

        return results;
    }


    public static void main(String[] args) throws SQLException {
        String stateCode = "50";
        TotalPopulationResponse population2010 = fetchTotalPopulation("state", stateCode, "2010");
        TotalPopulationResponse population2000 = fetchTotalPopulation("state", stateCode, "2000");
        TotalPopulationResponse population1990 = fetchTotalPopulation("state", stateCode, "1990");
        TotalPopulationResponse population1980 = fetchTotalPopulation("state", stateCode, "1980");
        log.info("Total Population for state " + stateCode + " in 2010: " + population2010.getPopulation());
        log.info("Total Population for state " + stateCode + " in 2000: " + population2000.getPopulation());
        log.info("Total Population for state " + stateCode + " in 1990: " + population1990.getPopulation());
        log.info("Total Population for state " + stateCode + " in 1980: " + population1980.getPopulation());

        // get states where population is greater than 10 million
        //HashMap<String, String> results = fetchTargetedInfo("2000", "county", ">", 1000000);
        //for (String geoId : results.keySet()) {
        //    log.info(geoId + ": " + results.get(geoId));
        //}

        HashMap<String, String> results = fetchTargetedInfo("2000", "tract", ">", 10000);
        for (String geoId : results.keySet()) {
            log.info(geoId + ": " + results.get(geoId));
        }
    }

}
