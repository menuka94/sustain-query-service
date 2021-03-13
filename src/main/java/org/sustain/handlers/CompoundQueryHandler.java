package org.sustain.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import org.bson.Document;
import org.sustain.CompoundResponse;
import org.sustain.CompoundRequest;
import org.sustain.Query;

import org.sustain.db.mongodb.queries.DataContainer;
import org.sustain.db.mongodb.queries.Querier;

import java.util.concurrent.LinkedBlockingQueue;

// TODO: Either rework or remove this
public class CompoundQueryHandler extends GrpcHandler<CompoundRequest, CompoundResponse> {

    public static final Logger log = LogManager.getLogger(CompoundQueryHandler.class);

    public CompoundQueryHandler(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
        super(request, responseObserver);
    }

    @Override
    public void handleRequest() {
        DataContainer compoundQueryResults = processCompoundQuery(this.request, true);
        compoundQueryResults.writeToClient(this.responseObserver);
        responseObserver.onCompleted();
    }

    public DataContainer processCompoundQuery(CompoundRequest request, boolean topLevel) {

        XStreamWriter  sw1 = null, sw2 = null;
        DataContainer dc1 = null, dc2 = null;

        // Evaluate first part of CompoundQuery
        switch (request.getFirstPredicateCase().getNumber()) {
            case 1: // Non-recursive type Query
                sw1 = startSingleQuery(request.getFirstQuery());
                break;
            case 2: // Recursive type CompoundRequest
                dc1 = processCompoundQuery(request.getFirstCompoundRequest(), false);
                break;
        }

        // Evaluate second part of CompoundQuery
        switch (request.getSecondPredicateCase().getNumber()) {
            case 4: // Non-recursive type Query
                sw2 = startSingleQuery(request.getSecondQuery());
                break;
            case 5: // Recursive type CompoundRequest
                dc2 = processCompoundQuery(request.getSecondCompoundRequest(), false);
                break;
        }

        // Wait for queries to complete
        try {
            if (sw1 != null) {
                sw1.join();
                dc1 = sw1.getDataContainer();
                if (sw2 != null) {
                    sw2.join();
                    dc2 = sw2.getDataContainer();
                }
            }
        } catch (InterruptedException e) {
            return new DataContainer();
        }

        // If we had multiple predicates, merge the results
        DataContainer dc = dc1;
        if (request.getSecondPredicateCase().getNumber() == 4 || request.getSecondPredicateCase().getNumber() == 5) {
            return dc1.innerJoin(dc2);
        }

        return dc;
    }

    private XStreamWriter startSingleQuery(Query q) {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        CompoundQueryHandler.XStreamWriter sw = new CompoundQueryHandler.XStreamWriter(responseObserver, queue);
        sw.start();
        new Querier(this, q, queue, sw).start();
        return sw;
    }

    class CompoundStreamWriter extends StreamWriter<Document, CompoundResponse> {

        public CompoundStreamWriter(
                StreamObserver<CompoundResponse> responseObserver,
                int threadCount) {
            super(responseObserver, threadCount);
        }

        @Override
        public CompoundResponse convert(Document document) {
            return CompoundResponse.newBuilder()
                    .setData(document.toJson())
                    .build();
        }
    }

    // TODO: Either rework or remove this
    public class XStreamWriter extends Thread {
        private final StreamObserver<CompoundResponse> responseObserver;
        private volatile LinkedBlockingQueue<String> data;
        private long start;
        private volatile boolean fetchingCompleted = false;
        private DataContainer dc;


        public XStreamWriter(StreamObserver<CompoundResponse> responseObserver, LinkedBlockingQueue<String> data) {
            this.responseObserver = responseObserver;
            this.data = data;
            this.start = System.currentTimeMillis();
        }

        public synchronized void setFetchingCompleted(boolean fetchingCompleted) {
            this.fetchingCompleted = fetchingCompleted;
        }

        public DataContainer getDataContainer() {
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
                    dc.addData(datum);
                    count++;
                }
            }

            for (String datum : data) {
                dc.addData(datum);
                count++;
            }
            log.info("Streaming completed! No. of entries: " + count);
            log.info("Milliseconds taken: " + (System.currentTimeMillis() - this.start));
        }

    }
}
