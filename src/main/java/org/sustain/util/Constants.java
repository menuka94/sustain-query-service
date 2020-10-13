package org.sustain.util;

import org.sustain.CensusResolution;
import org.sustain.DatasetRequest;
import org.sustain.Decade;
import org.sustain.OsmRequest;
import org.sustain.Predicate;

import java.util.HashMap;

public class Constants {
    public static final String EMPTY_COMPARISON_FIELD = "";
    public static final String GIS_JOIN = "GISJOIN";

    public static final HashMap<Decade, String> DECADES = new HashMap<Decade, String>() {{
        put(Decade._2010, "2010");
        put(Decade._2000, "2000");
        put(Decade._1990, "1990");
        put(Decade._1980, "1980");
    }};
    public static final HashMap<CensusResolution, String> TARGET_RESOLUTIONS =
            new HashMap<CensusResolution, String>() {{
                put(CensusResolution.State, CensusResolutions.STATE);
                put(CensusResolution.County, CensusResolutions.COUNTY);
                put(CensusResolution.Tract, CensusResolutions.TRACT);
                put(CensusResolution.Block, CensusResolutions.BLOCK);
            }};
    public static final HashMap<Predicate.ComparisonOperator, String> COMPARISON_OPS =
            new HashMap<Predicate.ComparisonOperator, String>() {{
                put(Predicate.ComparisonOperator.EQUAL, "=");
                put(Predicate.ComparisonOperator.GREATER_THAN, ">");
                put(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, ">=");
                put(Predicate.ComparisonOperator.LESS_THAN, "<");
                put(Predicate.ComparisonOperator.LESS_THAN_OR_EQUAL, "<=");
            }};
    public static final HashMap<OsmRequest.Dataset, String> OSM_DATASETS =
            new HashMap<OsmRequest.Dataset, String>() {{
                put(OsmRequest.Dataset.POINTS, "osm_points_geo");
                put(OsmRequest.Dataset.LINES, "osm_lines_geo");
                put(OsmRequest.Dataset.MULTI_LINES, "osm_multilines_geo");
                put(OsmRequest.Dataset.MULTI_POLYGONS, "osm_multipolygons_geo");
                put(OsmRequest.Dataset.OTHER, "osm_other_geo");
            }};
    public static final HashMap<DatasetRequest.Dataset, String> DATASETS =
            new HashMap<DatasetRequest.Dataset, String>() {{
                put(DatasetRequest.Dataset.DAMS, "dams_geo");
                put(DatasetRequest.Dataset.HOSPITALS, "hospitals_geo");
                put(DatasetRequest.Dataset.ELECTRICAL_SUBSTATIONS, "electrical_substations_geo");
                put(DatasetRequest.Dataset.POWER_PLANTS, "power_plants_geo");
                put(DatasetRequest.Dataset.NATURAL_GAS_PIPELINES, "natural_gas_pipelines_geo");
                put(DatasetRequest.Dataset.TRANSMISSION_LINES, "transmission_lines_geo");
                put(DatasetRequest.Dataset.FIRE_STATIONS, "fire_stations_geo");
                put(DatasetRequest.Dataset.FLOOD_ZONES, "flood_zones_geo");
            }};

    public static final class MongoDBCollections {
        public static final String TOTAL_POPULATION = "total_population";
        public static final String POPULATION_BY_AGE = "population_by_age";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "median_household_income";
    }

    public static class Server {
        public static final String HOST = "SERVER_HOST";
        public static final int PORT = 50055;
    }

    public static class GeoJsonCollections {
        public static final String STATES_GEO = "state_geo";
        public static final String COUNTIES_GEO = "county_geo";
        public static final String TRACTS_GEO = "tract_geo";
    }

    public static class DB {
        public static final String DB_NAME = "sustaindb";
        public static final String USERNAME = "DB_USERNAME";
        public static final String PASSWORD = "DB_PASSWORD";
        public static final String HOST = "DB_HOST";
        public static final String PORT = "DB_PORT";
    }

    public static class CensusFeatures {
        public static final String TOTAL_POPULATION = "total_population";
        public static final String MEDIAN_HOUSEHOLD_INCOME = "median_household_income";
        public static final String POPULATION_BY_AGE = "population_by_age";
        public static final String MEDIAN_AGE = "medianage";
        public static final String POVERTY = "poverty";
        public static final String RACE = "race";
    }

    public static class CensusResolutions {
        public static final String STATE = "state";
        public static final String COUNTY = "county";
        public static final String TRACT = "tract";
        public static final String BLOCK = "block";
    }
}
