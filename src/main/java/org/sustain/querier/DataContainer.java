package org.sustain.querier;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonParser;
import org.json.JSONObject;
import com.google.gson.Gson;

import org.sustain.CompoundResponse;
import org.sustain.CompoundRequest;

import io.grpc.stub.StreamObserver;

public class DataContainer {
	private HashMap<String,HashMap<String,String>> documents;

	public DataContainer() {
		this.documents = new HashMap<String,HashMap<String,String>>();
    }

	public DataContainer(HashMap<String,HashMap<String,String>> documents) {
		this.documents = documents;
    }

	public HashMap<String,HashMap<String,String>> getDocuments(){
		return documents;
	}

	public void addData(String datum){
	    System.out.println(JsonParser.parseString(datum).getAsJsonObject().keySet());
		String gisjoin = JsonParser.parseString(datum).getAsJsonObject().get("GISJOIN").toString().replace("\"", "");
		documents.put(gisjoin, new Gson().fromJson(datum, HashMap.class));
	}

	public DataContainer innerJoin(DataContainer other){
		documents.entrySet().forEach(entry->{
			System.out.println(entry.getKey() + " " + entry.getValue());  
		});

		HashMap<String,HashMap<String,String>> results = new HashMap<String,HashMap<String,String>>();

		for (Map.Entry<String, HashMap<String,String>> entry : other.getDocuments().entrySet()) {
			String key = entry.getKey();
			HashMap<String,String> value = entry.getValue();

			// Inner join - we only want GISJOINS in both queries
			if (documents.containsKey(key)){
				documents.get(key).forEach(value::putIfAbsent);
				results.put(key, value);
			}
		}
		
		return new DataContainer(results);
	}

	public void writeToClient(StreamObserver<CompoundResponse> responseObserver){
		for (HashMap<String,String> value : documents.values()) {
			responseObserver.onNext(CompoundResponse.newBuilder().setData(new JSONObject(value).toString()).setGeoJson("").build());
		}
	}
}