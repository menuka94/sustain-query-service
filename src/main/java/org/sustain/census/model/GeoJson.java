package org.sustain.census.model;

import com.google.gson.Gson;

public class GeoJson {
    private String type;
    private Properties properties;
    private Geometry geometry;

    public GeoJson() {

    }

    public void setType(String type) {
        this.type = type;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    public String getType() {
        return type;
    }

    public Properties getProperties() {
        return properties;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    @Override
    public String toString() {
        return "GeoJson{" +
                "type='" + type + '\'' +
                ", properties=" + properties +
                ", geometry=" + geometry +
                '}';
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
