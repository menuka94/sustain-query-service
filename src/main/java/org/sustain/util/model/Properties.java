package org.sustain.util.model;

import com.google.gson.annotations.SerializedName;

public class Properties {
    @SerializedName("STATEFP10")
    private String stateFp;

    @SerializedName("COUNTYFP10")
    private String countyFp;

    @SerializedName("TRACTFP10")
    private String tractFp;

    @SerializedName("GEOID10")
    private String GeoId;

    @SerializedName("NAME10")
    private String name;

    @SerializedName("NAMELSAD10")
    private String nameLsad;

    @SerializedName("GISJOIN")
    private String gisJoin;

    public Properties() {
    }

    public String getStateFp() {
        return stateFp;
    }

    public void setStateFp(String stateFp) {
        this.stateFp = stateFp;
    }

    public String getCountyFp() {
        return countyFp;
    }

    public void setCountyFp(String countyFp) {
        this.countyFp = countyFp;
    }

    public String getTractFp() {
        return tractFp;
    }

    public void setTractFp(String tractFp) {
        this.tractFp = tractFp;
    }

    public String getGeoId() {
        return GeoId;
    }

    public void setGeoId(String geoId) {
        GeoId = geoId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNameLsad() {
        return nameLsad;
    }

    public void setNameLsad(String nameLsad) {
        this.nameLsad = nameLsad;
    }

    public String getGisJoin() {
        return gisJoin;
    }

    public void setGisJoin(String gisJoin) {
        this.gisJoin = gisJoin;
    }

    @Override
    public String toString() {
        return "Properties{" +
                "stateFp='" + stateFp + '\'' +
                ", countyFp='" + countyFp + '\'' +
                ", tractFp='" + tractFp + '\'' +
                ", GeoId='" + GeoId + '\'' +
                ", name='" + name + '\'' +
                ", nameLsad='" + nameLsad + '\'' +
                ", gisJoin='" + gisJoin + '\'' +
                '}';
    }
}
