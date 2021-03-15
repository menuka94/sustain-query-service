package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sustain.Collection;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.modeling.LinearRegressionModelImpl;
import org.sustain.util.Constants;

import java.util.HashMap;
import java.util.Map;

public class RegressionQueryHandler extends ModelHandler {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver,
                                  SparkSession sparkSession) {
        super(request, responseObserver, sparkSession);
    }

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {
            logRequest(this.request);

            // Set parameters of Linear Regression Model
            LinearRegressionRequest lrRequest = this.request.getLinearRegressionRequest();
            Collection requestCollection = this.request.getCollections(0); // We only support 1 collection currently

            String mongoUri = String.format("mongodb://%s:%s", Constants.DB.HOST, Constants.DB.PORT);

            JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
            sparkContext.getConf()
                    .set("spark.mongodb.input.uri", mongoUri)
                    .set("spark.mongodb.input.database", Constants.DB.NAME)
                    .set("spark.mongodb.input.collection", requestCollection.getName());


            // Create a custom ReadConfig
            Map<String, String> readOverrides = new HashMap<String, String>();

            readOverrides.put("uri", mongoUri);
            readOverrides.put("database", Constants.DB.NAME);
            readOverrides.put("collection", requestCollection.getName());
            ReadConfig readConfig = ReadConfig.create(sparkContext).withOptions(readOverrides);

            // Lazy-load the collection in as a DF
            Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();

            // Build and run a model for each GISJoin in the request
            for (String gisJoin: lrRequest.getGisJoinsList()) {

                LinearRegressionModelImpl model = new LinearRegressionModelImpl.LinearRegressionModelBuilder()
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

                model.buildAndRunModel(); // Launches the Spark Model

                LinearRegressionResponse modelResults = LinearRegressionResponse.newBuilder()
                        .setGisJoin(model.getGisJoin())
                        .setTotalIterations(model.getTotalIterations())
                        .setRmseResidual(model.getRmse())
                        .setR2Residual(model.getR2())
                        .setIntercept(model.getIntercept())
                        .addAllSlopeCoefficients(model.getCoefficients())
                        .addAllObjectiveHistory(model.getObjectiveHistory())
                        .build();

                ModelResponse response = ModelResponse.newBuilder()
                        .setLinearRegressionResponse(modelResults)
                        .build();

                logResponse(response);
                this.responseObserver.onNext(response);
            }
            sparkContext.close();
        } else {
            log.warn("Invalid Model Request!");
        }
    }

    @Override
    boolean isValid(ModelRequest modelRequest) {
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
