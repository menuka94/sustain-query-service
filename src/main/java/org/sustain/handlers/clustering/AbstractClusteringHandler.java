package org.sustain.handlers.clustering;

import io.grpc.stub.StreamObserver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.sustain.ModelResponse;

public abstract class AbstractClusteringHandler {
    public abstract Dataset<Row> buildModel(int k, int maxIterations, Dataset<Row> featureDF);

    public abstract void buildModelWithPCA(int clusterCount, int maxIterations, int principalComponentCount, Dataset<Row> featureDF);

    public abstract void writeToStream(Dataset<Row> predictDF, StreamObserver<ModelResponse> responseObserver);
}
