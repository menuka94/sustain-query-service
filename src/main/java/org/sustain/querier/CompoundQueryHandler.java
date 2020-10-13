package org.sustain.querier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonParser;

import io.grpc.stub.StreamObserver;
import org.sustain.CompoundResponse;
import org.sustain.CompoundRequest;
import org.sustain.Query;

import org.sustain.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class CompoundQueryHandler {
    public static final Logger log = LogManager.getLogger(CompoundQueryHandler.class);

    private final CompoundRequest request;
    private final StreamObserver<CompoundResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public CompoundQueryHandler(CompoundRequest request,
                                      StreamObserver<CompoundResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleCompoundQuery() {
        //HashMap<String, String> geoJsonMap = SpatialQueryUtil.getGeoList(requestGeoJson, resolution, spatialOp);
        if(this.request.getFirstPredicateCase().getNumber() == 1){
            LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
            new CompoundQueryHandler.StreamWriter(responseObserver, queue).start();
            Querier.executeQuery(this.request.getFirstQuery(), queue);
            fetchingCompleted = true;
        }
    }


    public class StreamWriter extends Thread {
        private final StreamObserver<CompoundResponse> responseObserver;
        private volatile LinkedBlockingQueue<String> data;


        public StreamWriter(StreamObserver<CompoundResponse> responseObserver, LinkedBlockingQueue<String> data) {
            this.responseObserver = responseObserver;
            this.data = data;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
            while (!fetchingCompleted) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    writeDataToStream(datum);
                    count++;
                }
            }

            for (String datum : data) {
                writeDataToStream(datum);
                count++;
            }
            log.info("Streaming completed! No. of entries: " + count);
            responseObserver.onCompleted();
        }

        private void writeDataToStream(String datum) {
            //String jsonDatum = JsonParser.parseString(datum).getAsJsonObject().get(
            //        Constants.GIS_JOIN).toString().replace("\"", "");
            //String responseGeoJson = geoJsonMap.get(gisJoinInDataRecord);
            responseObserver.onNext(CompoundResponse.newBuilder().setData(datum).setGeoJson("").build());
        }
    }
}