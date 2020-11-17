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

    private final StreamObserver<CompoundResponse> responseObserver;
    

    public CompoundQueryHandler(StreamObserver<CompoundResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public DataContainer handleCompoundQuery(CompoundRequest request, boolean topLevel) {
        // If the first predicate is a MongoDB query or nested compound query
        StreamWriter sw1;
        DataContainer dc1;
        if(request.getFirstPredicateCase().getNumber() == 1)
            sw1 = startSingleQuery(request.getFirstQuery());
        else if(request.getFirstPredicateCase().getNumber() == 2)
            dc1 = handleCompoundQuery(request.getFirstCompoundRequest(), false);

        // If the second predicate is a MongoDB query or nested compound query
        StreamWriter sw2;
        DataContainer dc2;
        if(request.getSecondPredicateCase().getNumber() == 4)
            sw2 = startSingleQuery(request.getSecondQuery());
        else if(request.getSecondPredicateCase().getNumber() == 5)
            dc2 = handleCompoundQuery(request.getSecondCompoundRequest(), false);

        // Wait for queries to complete
        if(sw1 != null){
            sw1.join();
            dc1 = sw1.getDataContainer();
            if(sw2 != null){
                sw2.join();
                dc2 = sw2.getDataContainer();
            }
        }

        // If we had multiple predicates, merge the results
        DataContainer dc = sw1.getDataContainer();
        if(request.getSecondPredicateCase().getNumber() == 4 || request.getSecondPredicateCase().getNumber() == 5)
            dc = dc.innerJoin(sw2.getDataContainer());  
            
        // If this is the top of the tree (final query(s))
        if(topLevel)
            dc.writeToClient(this.responseObserver);
        return dc;
    }

    private StreamWriter startSingleQuery(Query q){
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        CompoundQueryHandler.StreamWriter sw = new CompoundQueryHandler.StreamWriter(responseObserver, queue);
        sw.start();
        new Querier(this, q, queue, sw).start();
        return sw;
	}


    public class StreamWriter extends Thread {
        private final StreamObserver<CompoundResponse> responseObserver;
        private volatile LinkedBlockingQueue<String> data;
        private long start;
        private volatile boolean fetchingCompleted = false;
        private DataContainer dc;


        public StreamWriter(StreamObserver<CompoundResponse> responseObserver, LinkedBlockingQueue<String> data) {
            this.responseObserver = responseObserver;
            this.data = data;
            this.start = System.currentTimeMillis();
        }

        public synchronized void setFetchingCompleted(boolean fetchingCompleted){
            this.fetchingCompleted = fetchingCompleted;
		}

        public DataContainer getDataContainer(){
            return dc;  
		}

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            int count = 0;
            dc = new DataContainer();
            long start = System.currentTimeMillis();
            while (!fetchingCompleted && System.currentTimeMillis() - start < 10000) {
                if (data.size() > 0) {
                    String datum = data.remove();
                    writeDataToStream(datum);
                    //dc.addData(datum);
                    count++;
                }
            }

            for (String datum : data) {
                writeDataToStream(datum);
                //dc.addData(datum);
                count++;
            }
            log.info("Streaming completed! No. of entries: " + count);
            log.info("Milliseconds taken: " + (System.currentTimeMillis() - this.start));
            //dc.innerJoin();
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