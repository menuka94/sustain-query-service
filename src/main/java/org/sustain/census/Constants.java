package org.sustain.census;

import java.util.HashMap;

public class Constants {
    public static final HashMap<SpatialTemporalInfo.Decade, String> DECADES =
            new HashMap<SpatialTemporalInfo.Decade, String>() {{
                put(SpatialTemporalInfo.Decade._2010, "2010");
                put(SpatialTemporalInfo.Decade._2000, "2000");
                put(SpatialTemporalInfo.Decade._1990, "1990");
                put(SpatialTemporalInfo.Decade._1980, "1980");
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
        public static final String TOTAL_POPULATION = "population";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "medianhouseholdincome";
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
