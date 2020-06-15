package org.sustain.census;

import java.util.HashMap;

public class Constants {
    public static final String EMPTY_COMPARISON_FIELD = "";

    public static final HashMap<Decade, String> DECADES = new HashMap<Decade, String>() {{
        put(Decade._2010, "2010");
        put(Decade._2000, "2000");
        put(Decade._1990, "1990");
        put(Decade._1980, "1980");
    }};

    public static final class MongoDBCollections {
        public static final String GEO_IDS = "geoids";
        public static final String TOTAL_POPULATION = "total_population";
        public static final String POPULATION_BY_AGE = "population_by_age";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "median_household_income";
    }

    public static final HashMap<Predicate.Feature, String> TARGET_FEATURES = new HashMap<Predicate.Feature, String>() {{
        put(Predicate.Feature.Population, CensusFeatures.POPULATION);
        put(Predicate.Feature.Income, CensusFeatures.MEDIAN_HOUSEHOLD_INCOME);
    }};

    public static final HashMap<CensusResolution, String> TARGET_RESOLUTIONS =
            new HashMap<CensusResolution, String>() {{
                put(CensusResolution.State, CensusResolutions.STATE);
                put(CensusResolution.County, CensusResolutions.COUNTY);
                put(CensusResolution.Tract, CensusResolutions.TRACT);
            }};

    public static final HashMap<Predicate.ComparisonOperator, String> COMPARISON_OPS =
            new HashMap<Predicate.ComparisonOperator, String>() {{
                put(Predicate.ComparisonOperator.EQUAL, "=");
                put(Predicate.ComparisonOperator.GREATER_THAN, ">");
                put(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, ">=");
                put(Predicate.ComparisonOperator.LESS_THAN, "<");
                put(Predicate.ComparisonOperator.LESS_THAN_OR_EQUAL, "<=");
            }};


    public static class Server {
        public static final String HOST = "SERVER_HOST";
        public static final int PORT = 50051;
    }

    public static class DB {
        public static final String DB_NAME = "sustain_census";
        public static final String USERNAME = "DB_USERNAME";
        public static final String PASSWORD = "DB_PASSWORD";
        public static final String HOST = "DB_HOST";
    }

    public static class CensusFeatures {
        public static final String POPULATION = "population";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "median_household_income";
        public static final String POPULATION_BY_AGE = "population_by_age";
        public static final String MEDIAN_AGE = "medianage";
        public static final String POVERTY = "poverty";
        public static final String RACE = "race";
    }

    public static class CensusResolutions {
        public static final String GEO_ID = "geoid";
        public static final String STATE = "state";
        public static final String COUNTY = "county";
        public static final String TRACT = "tract";
        public static final String BLOCK = "block";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";
    }
}
