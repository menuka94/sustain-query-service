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
import org.sustain.SparkTask;
import org.sustain.modeling.LRModel;
import org.sustain.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class RegressionQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {
            logRequest(this.request);
			try {

				// For each GISJoin in the request, submit a task to Spark Manager
				List<Future<ModelResponse>> lrModelTasks = new ArrayList<>();
				for (String gisJoin: this.request.getLinearRegressionRequest().getGisJoinsList()) {
					LinearRegressionTask lrTask = new LinearRegressionTask(this.request, gisJoin);
					lrModelTasks.add(this.sparkManager.submit(lrTask, "regression-query"));
				}

				// Wait for each task to complete and return their ModelResponses
				for (Future<ModelResponse> lrModelTask: lrModelTasks) {
					this.responseObserver.onNext(lrModelTask.get());
				}

			} catch (Exception e) {
				log.error("Failed to evaluate query", e);
				responseObserver.onError(e);
			}
        } else {
            log.warn("Invalid Model Request!");
        }
    }

    protected class LinearRegressionTask implements SparkTask<ModelResponse> {

		private final LinearRegressionRequest lrRequest;
		private final Collection requestCollection;
    	private final String gisJoin;

    	LinearRegressionTask(ModelRequest modelRequest, String gisJoin) {
			this.lrRequest = modelRequest.getLinearRegressionRequest();
			this.requestCollection = modelRequest.getCollections(0); // We only support 1 collection currently
			this.gisJoin = gisJoin;
		}

		@Override
		public ModelResponse execute(JavaSparkContext sparkContext) throws Exception {

			// Create a custom Mongo-Spark ReadConfig
			Map<String, String> readOverrides = new HashMap<String, String>();
			String mongoUri = String.format("mongodb://%s:%s", Constants.DB.HOST, Constants.DB.PORT);
			readOverrides.put("uri", mongoUri);
			readOverrides.put("database", Constants.DB.NAME);
			readOverrides.put("collection", requestCollection.getName());
			ReadConfig readConfig = ReadConfig.create(sparkContext.getConf(), readOverrides);

			// Lazy-load the collection in as a DF
			Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();

			LRModel model = new LRModel.LRModelBuilder()
					.forMongoCollection(mongoCollection)
					.forGISJoin(gisJoin)
					.forFeatures(requestCollection.getFeaturesList())
					.forLabel(requestCollection.getLabel())
					.withLoss(lrRequest.getLoss())
					.withSolver(lrRequest.getSolver())
					.withAggregationDepth(lrRequest.getAggregationDepth())
					.withMaxIterations(lrRequest.getMaxIterations())
					.withElasticNetParam(lrRequest.getElasticNetParam())
					.withEpsilon(lrRequest.getEpsilon())
					.withRegularizationParam(lrRequest.getRegularizationParam())
					.withTolerance(lrRequest.getConvergenceTolerance())
					.withFitIntercept(lrRequest.getFitIntercept())
					.withStandardization(lrRequest.getSetStandardization())
					.build();

			model.train(); // Launches the Spark Model

			// Build model response and return it
			LinearRegressionResponse modelResults = LinearRegressionResponse.newBuilder()
					.setGisJoin(model.getGisJoin())
					.setTotalIterations(model.getTotalIterations())
					.setRmseResidual(model.getRmse())
					.setR2Residual(model.getR2())
					.setIntercept(model.getIntercept())
					.addAllSlopeCoefficients(model.getCoefficients())
					.addAllObjectiveHistory(model.getObjectiveHistory())
					.build();

			return ModelResponse.newBuilder()
					.setLinearRegressionResponse(modelResults)
					.build();
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
