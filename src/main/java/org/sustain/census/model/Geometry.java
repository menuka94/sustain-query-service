package org.sustain.census.model;

import com.google.gson.JsonArray;

public class Geometry {
    private String type;
    private JsonArray coordinates;

    public Geometry(String type, JsonArray coordinates) {
        this.type = type;
        this.coordinates = coordinates;
    }

    public String getType() {
        return type;
    }

    public JsonArray getCoordinates() {
        return coordinates;
    }

    @Override
    public String toString() {
        return "Geometry{" +
                "type='" + type + '\'' +
                ", coordinates=" + coordinates +
                '}';
    }
}

