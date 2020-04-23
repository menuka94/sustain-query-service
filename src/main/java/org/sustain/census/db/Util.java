package org.sustain.census.db;

public class Util {
    public static String constructTableName(String aspect, String resolution) {
        return resolution + "_" + aspect;
    }
}
