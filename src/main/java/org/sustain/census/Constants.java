package org.sustain.census;

public class Constants {
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
        public static final String TOTAL_POPULATION = "total_population";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "medianhouseholdincome";
        public static final String POPULATION_BY_AGE = "population_by_age";
    }

    public static class CensusResolutions {
        public static final String STATE = "state";
        public static final String COUNTY = "county";
        public static final String TRACT = "tract";
    }
}
