package org.sustain.util;

import org.sustain.CensusResolution;
import org.sustain.ComparisonOperator;

import java.util.HashMap;

public class Constants {
    public static final String GIS_JOIN = "GISJOIN";

    public static final HashMap<CensusResolution, String> TARGET_RESOLUTIONS =
            new HashMap<CensusResolution, String>() {{
                put(CensusResolution.State, CensusResolutions.STATE);
                put(CensusResolution.County, CensusResolutions.COUNTY);
                put(CensusResolution.Tract, CensusResolutions.TRACT);
                put(CensusResolution.Block, CensusResolutions.BLOCK);
            }};

    public static final HashMap<ComparisonOperator, String> COMPARISON_OPS =
            new HashMap<ComparisonOperator, String>() {{
                put(ComparisonOperator.EQUAL, "=");
                put(ComparisonOperator.GREATER_THAN, ">");
                put(ComparisonOperator.GREATER_THAN_OR_EQUAL, ">=");
                put(ComparisonOperator.LESS_THAN, "<");
                put(ComparisonOperator.LESS_THAN_OR_EQUAL, "<=");
            }};

    public static class Server {
        public static final String  HOST = System.getenv("SERVER_HOST");
        public static final Integer PORT = Integer.parseInt(System.getenv("SERVER_PORT"));
    }

    public static class GeoJsonCollections {
        public static final String STATES_GEO   = "state_geo";
        public static final String COUNTIES_GEO = "county_geo";
        public static final String TRACTS_GEO   = "tract_geo";
    }

    public static class DB {
        public static final String  NAME     = System.getenv("DB_NAME");
        public static final String  USERNAME = System.getenv("DB_USERNAME");
        public static final String  PASSWORD = System.getenv("DB_PASSWORD");
        public static final String  HOST     = System.getenv("DB_HOST");
        public static final Integer PORT     = Integer.parseInt(System.getenv("DB_PORT"));
    }

    public static class CensusResolutions {
        public static final String STATE  = "state";
        public static final String COUNTY = "county";
        public static final String TRACT  = "tract";
        public static final String BLOCK  = "block";
    }

    public static class Spark {
        public static final String MASTER = System.getenv("SPARK_MASTER");
    }
}
