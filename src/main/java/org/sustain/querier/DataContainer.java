package org.sustain.querier;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.sustain.CompoundResponse;

import java.util.HashMap;
import java.util.Map;

public class DataContainer {
    private static final Logger log = LogManager.getLogger(DataContainer.class);
    private HashMap<String, HashMap<String, String>> documents;

    public DataContainer() {
        this.documents = new HashMap<String, HashMap<String, String>>();
    }

    public DataContainer(HashMap<String, HashMap<String, String>> documents) {
        this.documents = documents;
    }

    public HashMap<String, HashMap<String, String>> getDocuments() {
        return documents;
    }

    public void addData(String datum) {
        log.info("DATUM: " + datum);
        if (JsonParser.parseString(datum).getAsJsonObject().has("GISJOIN")) {
            String gisjoin = JsonParser.parseString(datum).getAsJsonObject().get("GISJOIN").toString().replace("\"",
                    "");
            documents.put(gisjoin, new Gson().fromJson(datum, HashMap.class));
        } else {
            log.info("GISJOIN not found");
            documents.put("", new Gson().fromJson(datum, HashMap.class));
        }
    }

    public DataContainer innerJoin(DataContainer other) {
        //documents.entrySet().forEach(entry->{
        //	System.out.println(entry.getKey() + " " + entry.getValue());
        //});

        HashMap<String, HashMap<String, String>> results = new HashMap<String, HashMap<String, String>>();

        for (Map.Entry<String, HashMap<String, String>> entry : other.getDocuments().entrySet()) {
            String key = entry.getKey();
            //System.out.println(key);
            HashMap<String, String> value = entry.getValue();

            // Inner join - we only want GISJOINS in both queries
            if (documents.containsKey(key)) {
                //System.out.println(key);
                value.putAll(documents.get(key));
                results.put(key, value);
            }
        }

        return new DataContainer(results);
    }

    public void writeToClient(StreamObserver<CompoundResponse> responseObserver) {
        for (HashMap<String, String> value : documents.values()) {
            responseObserver.onNext(CompoundResponse.newBuilder().setData(new JSONObject(value).toString()).setGeoJson("").build());
        }
    }
}
