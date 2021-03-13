package org.sustain.db.mongodb.queries;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.gson.JsonParser;
import org.json.JSONObject;
import com.google.gson.Gson;

import org.sustain.CompoundResponse;

import io.grpc.stub.StreamObserver;

// TODO: Either rework or remove this
public class DataContainer {

	private HashMap<String,HashMap<String,String>> documents;

	public DataContainer() {
		this(new HashMap<>());
    }

	public DataContainer(HashMap<String,HashMap<String,String>> documents) {
		this.documents = documents;
    }

	public HashMap<String,HashMap<String,String>> getDocuments(){
		return documents;
	}

	public void addData(String datum){
		if (JsonParser.parseString(datum).getAsJsonObject().has("GISJOIN")) {
			String gisjoin = JsonParser.parseString(datum).getAsJsonObject().get("GISJOIN").toString().replace("\"",
					"");
			documents.put(gisjoin, new Gson().fromJson(datum, HashMap.class));
		} else {
			String uniqueID = UUID.randomUUID().toString();
			documents.put(uniqueID, new Gson().fromJson(datum, HashMap.class));
		}
	}

	public DataContainer innerJoin(DataContainer other){

		HashMap<String,HashMap<String,String>> results = new HashMap<String,HashMap<String,String>>();

		for (Map.Entry<String, HashMap<String,String>> entry : other.getDocuments().entrySet()) {
			String key = entry.getKey();
			HashMap<String,String> value = entry.getValue();

			// Inner join - we only want GISJOINS in both queries
			if (documents.containsKey(key)){
				value.putAll(documents.get(key));
				results.put(key, value);
			}
		}
		
		return new DataContainer(results);
	}

	public void writeToClient(StreamObserver<CompoundResponse> responseObserver){
		for (HashMap<String,String> value : documents.values()) {
			responseObserver.onNext(CompoundResponse.newBuilder().setData(new JSONObject(value).toString()).build());
		}
	}
}
