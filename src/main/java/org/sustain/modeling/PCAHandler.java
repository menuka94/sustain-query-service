package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.feature.PCA;
import org.apache.spark.mllib.feature.PCAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.DummyResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.handlers.GrpcSparkHandler;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class PCAHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {
    private static final Logger log = LogManager.getFormatterLogger(PCAHandler.class);

    public PCAHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext) throws Exception {
        doPca(sparkContext);
        return true;
    }

    private void doPca(JavaSparkContext sparkContext) {
        // Initialize mongodb read configuration
        Map<String, String> readOverrides = new HashMap();
        readOverrides.put("spark.mongodb.input.collection", "svi_county_GISJOIN");
        readOverrides.put("spark.mongodb.input.database", "sustaindb");
        readOverrides.put("spark.mongodb.input.uri",
            "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig =
            ReadConfig.create(sparkContext.getConf(), readOverrides);

        // Load mongodb rdd and convert to dataset
        log.info("Preprocessing data");
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        Seq<String> features = convertListToSeq(featuresList);

        Dataset<Row> selectedFeatures = collection.select(Constants.GIS_JOIN, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

        //selectedFeatures.show(10);

        // Assembling
        VectorAssembler assembler =
            new VectorAssembler().setInputCols(featuresList.toArray(new String[0])).setOutputCol("features");
        Dataset<Row> featureDF = assembler.transform(selectedFeatures);
        featureDF.show(10);

        // Scaling
        log.info("Normalizing features");
        StandardScaler scaler = new StandardScaler()
            .setInputCol("features")
            .setOutputCol("normalized_features");
        StandardScalerModel scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        log.info("Dataframe after normalizing with StandardScaler");
        //featureDF.show(10);

        // convert scaled features into an MLlibRowMatrix
        Dataset<Row> scaledDf = featureDF.select("features");
        scaledDf.show(10);
        JavaRDD<Vector> vectorRDD = scaledDf.javaRDD()
            .map((Function<Row, Vector>) row -> (Vector) row.get(0));

        RowMatrix matrix = new RowMatrix(vectorRDD.rdd());

        Matrix pc = matrix.computePrincipalComponents(featuresList.size());
        log.info(pc);

        //RowMatrix projected = matrix.multiply(pc);
        //projected.log();

        //PCAModel pcaModel = new PCA(featuresList.size()).fit(vectorRDD);

        log.info("Completed");
        responseObserver.onNext(ModelResponse.newBuilder()
            .setDummyResponse(
                DummyResponse.newBuilder()
                    .setResult("completed!")
                    .build())
            .build());
    }

    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    @Override
    public void handleRequest() {
        this.logRequest(request);

        try {
            // Submit task to Spark Manager
            Future<Boolean> future =
                this.sparkManager.submit(this, "clustering-query");

            // Wait for task to complete
            future.get();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }
}
