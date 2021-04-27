package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.PCAResponse;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.util.Constants;
import org.sustain.util.ProfilingUtil;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class PCAHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {
    private static final Logger log = LogManager.getLogger(PCAHandler.class);

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
        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        Seq<String> features = convertListToSeq(featuresList);

        String collectionName = request.getCollections(0).getName();
        log.info("collectionName: {}", collectionName);

        Map<String, String> readOverrides = new HashMap<>();
        readOverrides.put("spark.mongodb.input.collection", collectionName);
        readOverrides.put("spark.mongodb.input.database", "sustaindb");
        readOverrides.put("spark.mongodb.input.uri",
            "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig =
            ReadConfig.create(sparkContext.getConf(), readOverrides);

        String gisJoin;
        if (collectionName.equals("noaa_nam")) {
            gisJoin = "gis_join";
        } else {
            gisJoin = Constants.GIS_JOIN;
        }

        // Load mongodb rdd and convert to dataset
        log.info("Preprocessing data");
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();

        Dataset<Row> selectedFeatures = collection.select(gisJoin, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

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
        featureDF.show(10);

        double samplingPercentage = request.getPcaRequest().getSamplingPercentage();

        featureDF = featureDF.sample(samplingPercentage);

        // PCA
        long pcaTime1 = System.currentTimeMillis();
        PCAModel pca = new PCA()
            .setInputCol("features")
            .setOutputCol("pcaFeatures")
            .setK(featuresList.size())
            .fit(featureDF);
        DenseMatrix pc = pca.pc();

        Dataset<Row> pcaDF = pca.transform(featureDF).select("features", "pcaFeatures");
        long pcaTime2 = System.currentTimeMillis();
        ProfilingUtil.calculateTimeDiff(pcaTime1, pcaTime2,
                String.format("PCA Time for collection '%s' with %f sampling", collectionName, samplingPercentage));
        //pcaDF.write().json("sustain-pcaDF.json");
        int requiredNoOfPCs = getNoOfPrincipalComponentsByVariance(pca, 0.95);
        log.info("requiredNoOfPCs: {}", requiredNoOfPCs);
        ProfilingUtil.writeToFile("requiredNoOfPCs: " + requiredNoOfPCs);

        pcaDF.show();
        //ProfilingUtil.writeToFile("PCA Results DataFrame: " + pcaDF.showString(100, 100, true));
        log.info("Size of results: ({}, {})", pcaDF.count(), pcaDF.columns().length);

        log.info("Completed");
        responseObserver.onNext(ModelResponse.newBuilder()
            .setPcaResponse(
                PCAResponse.newBuilder()
                    .setResult("completed!")
                    .build())
            .build());
    }

    /**
     * pca: PCA object
     * targetVariance: required variance
     *
     * @return : minimum number of principal components that account for the required variance
     */
    public int getNoOfPrincipalComponentsByVariance(PCAModel pca, double targetVariance) {
        int n;
        double varianceSum = 0.0;
        DenseVector explainedVariance = pca.explainedVariance();
        Iterator<Tuple2<Object, Object>> iterator = explainedVariance.iterator();
        while (iterator.hasNext()) {
            Tuple2<Object, Object> next = iterator.next();
            n = Integer.parseInt(next._1().toString()) + 1;
            if (n >= pca.getK()) {
                break;
            }
            varianceSum += Double.parseDouble(next._2().toString());
            if (varianceSum >= targetVariance) {
                return n;
            }
        }

        return pca.getK();
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
                this.sparkManager.submit(this, "pca-query");

            // Wait for task to complete
            future.get();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }
}
