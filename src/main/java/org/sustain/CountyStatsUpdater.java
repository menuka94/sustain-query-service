package org.sustain;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.sustain.mongodb.DBConnection;

public class CountyStatsUpdater {
    /**
     * {
     *         "_id" : ObjectId("6024fe7dc1d226e5c0e006dd"),
     *         "gis_join" : "G0100130",
     *         "timestamp" : 788918400,
     *         "min_specific_humidity" : 0.004,
     *         "max_specific_humidity" : 0.004,
     *         "min_precipitation" : 0,
     *         "max_precipitation" : 0,
     *         "min_surface_downwelling_shortwave_flux_in_air" : 141.275,
     *         "max_surface_downwelling_shortwave_flux_in_air" : 146.407,
     *         "min_max_air_temperature" : 290.7,
     *         "max_max_air_temperature" : 291.954,
     *         "min_min_air_temperature" : 272.926,
     *         "max_min_air_temperature" : 274.616,
     *         "min_eastward_wind" : 1.254,
     *         "max_eastward_wind" : 2.37,
     *         "min_northward_wind" : -3.341,
     *         "max_northward_wind" : -2.262,
     *         "min_vpd" : 71,
     *         "max_vpd" : 79
     * }
     * @param args
     */
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection("lattice-100", 27018, "sustaindb");
        FindIterable<Document> macav2 = db.getCollection("macav2").find();
        for (Document document : macav2) {
            String gisJoin = document.getString("gis_join");
            Double minPrecipitation = document.getDouble("min_precipitation");
            String output = String.format("%s: %s", gisJoin, minPrecipitation);
            MongoCollection<Document> countyStats = db.getCollection("county_stats");
            countyStats.updateOne(Filters.eq("GISJOIN", gisJoin), new Document("$set",
                    new Document("min_precipitation", minPrecipitation)));
        }
    }
}
