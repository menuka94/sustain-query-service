package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.tasks.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Handler for the gRPC Regression requests.
 * handleRequest() is invoked when a LinearRegressionRequest is received by the gRPC service.
 */
public abstract class RegressionQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request,
								  StreamObserver<ModelResponse> responseObserver,
								  SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

	/**
	 * Batches a List of GISJOINs into a List of Lists based on the batchSize.
	 * @param gisJoins Total list of GISJOINs to batch
	 * @param batchSize Size of each batch
	 * @return A List of GISJOIN String batches
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

				// Create batches of GISJOINs
				List<List<String>> gisJoinBatches = batchGisJoins(
					this.request.getLinearRegressionRequest().getGisJoinsList(),
					20
				);

				// Create RegressionTasks for each of the GISJOIN batches, and submit them to Spark
				List<Future<List<ModelResponse>>> batchedTasks = new ArrayList<>();
				for (List<String> gisJoinBatch: gisJoinBatches) {
					batchedTasks.add(
						this.sparkManager.submit(
							this.createRegressionTask(gisJoinBatch), "regression-query"
						)
					);
				}

				// Wait for each task to complete and return their ModelResponses
				for (Future<List<ModelResponse>> completedTask: batchedTasks) {
					List<ModelResponse> batchedModelResponses = completedTask.get();
					for (ModelResponse modelResponse: batchedModelResponses) {
						this.responseObserver.onNext(modelResponse);
					}
				}

			} catch (Exception e) {
				log.error("Failed to evaluate query", e);
				this.responseObserver.onError(e);
			}
        } else {
            log.warn("Invalid Model Request!");
        }
    }

	/**
	 * Checks the validity of the gRPC ModelRequest according to the type of Regression requested.
	 * @param modelRequest gRPC ModelRequest object.
	 * @return True if valid, false if not.
	 */
    @Override
    public boolean isValid(ModelRequest modelRequest) {
		if (modelRequest.getCollectionsCount() == 1) {
			return this.hasAppropriateRegressionRequest(modelRequest);
		}
		log.error("isValid(ModelRequest): Expected 1 collection, got {}", modelRequest.getCollectionsCount());
        return false; // Must have exactly 1 collection
    }

	/**
	 * Determines if the appropriate Regression Request has been included in the model request,
	 * using the concrete subclass instance.
	 * @param modelRequest gRPC ModelRequest object, which should contain an appropriate Regression Request object.
	 * @return True if the appropriate object is included in the request, false if not.
	 */
	public abstract boolean hasAppropriateRegressionRequest(ModelRequest modelRequest);

	/**
	 * Creates an appropriate RegressionTask concrete instance, as defined by the subclass implementation.
	 * @param gisJoinBatch The batch of GISJOINs for the task.
	 * @return A concrete instance of a RegressionTask.
	 */
	public abstract RegressionTask createRegressionTask(List<String> gisJoinBatch);
}
