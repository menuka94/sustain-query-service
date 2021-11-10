package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.Collection;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.SparkManager;
import org.sustain.handlers.tasks.LinearRegressionTask;
import org.sustain.handlers.tasks.SparkTask;
import org.sustain.modeling.LRModel;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Handler for the gRPC Regression requests.
 * handleRequest() is invoked when a LinearRegressionRequest is received by the gRPC service.
 */
public class RegressionQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request,
								  StreamObserver<ModelResponse> responseObserver,
								  SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

	/**
	 * Batches a List of GISJOINs into a List of Lists based on the batchSize.
	 * @param gisJoins
	 * @param batchSize
	 * @return
	 */
    private List<List<String>> batchGisJoins(List<String> gisJoins, int batchSize) {
		List<List<String>> batches = new ArrayList<>();
		int totalGisJoins = gisJoins.size();
		int gisJoinsPerBatch = (int) Math.ceil((1.0 * totalGisJoins) / (1.0 * batchSize));
		log.info(">>> Max batch size: {}, totalGisJoins: {}, gisJoinsPerBatch: {}", batchSize, totalGisJoins,
				gisJoinsPerBatch);

		for (int i = 0; i < totalGisJoins; i++) {
			if ( i % gisJoinsPerBatch == 0 ) {
				batches.add(new ArrayList<>());
			}
			String gisJoin = gisJoins.get(i);
			batches.get(batches.size() - 1).add(gisJoin);
		}

		StringBuilder batchLog = new StringBuilder(
				String.format(">>> %d batches for %d GISJoins\n", batches.size(), totalGisJoins)
		);
		for (int i = 0; i < batches.size(); i++) {
			batchLog.append(String.format("\tBatch %d size: %d\n", i, batches.get(i).size()));
		}
		log.info(batchLog.toString());
		return batches;
	}

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {
            logRequest(this.request);
			try {

				// For each batch of GISJoins in the request, submit a task to Spark Manager
				List<List<String>> gisJoinBatches = batchGisJoins(
						this.request.getLinearRegressionRequest().getGisJoinsList(),
						20
				);

				List<Future<List<ModelResponse>>> batchedModelTasks = new ArrayList<>();
				for (List<String> gisJoinBatch: gisJoinBatches) {
					LinearRegressionTask lrTask = new LinearRegressionTask(this.request, gisJoinBatch);
					batchedModelTasks.add(this.sparkManager.submit(lrTask, "regression-query"));
				}

				// Wait for each task to complete and return their ModelResponses
				for (Future<List<ModelResponse>> lrModelTask: batchedModelTasks) {
					List<ModelResponse> batchedModelResponses = lrModelTask.get();
					for (ModelResponse modelResponse: batchedModelResponses) {
						this.responseObserver.onNext(modelResponse);
					}
				}

			} catch (Exception e) {
				log.error("Failed to evaluate query", e);
				responseObserver.onError(e);
			}
        } else {
            log.warn("Invalid Model Request!");
        }
    }

    @Override
    public boolean isValid(ModelRequest modelRequest) {
        if (modelRequest.getType().equals(ModelType.LINEAR_REGRESSION)) {
            if (modelRequest.getCollectionsCount() == 1) {
                if (modelRequest.getCollections(0).getFeaturesCount() == 1) {
                    return modelRequest.hasLinearRegressionRequest();
                }
            }
        }
        return false;
    }
}
