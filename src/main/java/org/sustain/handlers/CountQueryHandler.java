package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import io.grpc.stub.StreamObserver;

import org.apache.spark.api.java.JavaSparkContext;

import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sustain.CountResponse;
import org.sustain.CountRequest;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.server.SustainServer;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class CountQueryHandler extends GrpcSparkHandler<CountRequest, CountResponse> {
    protected static final Logger log =
        LoggerFactory.getLogger(CountQueryHandler.class);

    public CountQueryHandler(CountRequest request,
            StreamObserver<CountResponse> responseObserver,
            SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public void handleRequest() {
        try {
            // submit a CountSparkTask for each collection
            List<Future<Long>> futures = new ArrayList();
            for (String collection : request.getCollectionsList()) {
                // initialize CountSparkTask
                CountSparkTask countSparkTask = new CountSparkTask(collection);

                // submit task to SparkManager
                Future<Long> future = this.sparkManager.submit(
                    countSparkTask, "count-query");

                futures.add(future);
            }

            // wait for all SparkTasks to complete
            CountResponse.Builder response = CountResponse.newBuilder();
            for (Future<Long> future : futures) {
                long count = future.get();
                response.addCount(count);
            }

            // send response
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    protected class CountSparkTask implements SparkTask<Long> {
        protected String collection;

        public CountSparkTask(String collection) {
            this.collection = collection;
        }

        @Override
        public Long execute(JavaSparkContext sparkContext) throws Exception {
            // initialize ReadConfig
            Map<String, String> readOverrides = new HashMap();
            readOverrides.put("spark.mongodb.input.collection", this.collection);
            readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
            readOverrides.put("spark.mongodb.input.uri",
                "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

            ReadConfig readConfig = 
                ReadConfig.create(sparkContext.getConf(), readOverrides);

            // load RDD
            JavaMongoRDD<Document> rdd =
                MongoSpark.load(sparkContext, readConfig);

            // perform count evaluation
            long count = rdd.count();
            return count;
        }
    }
}
