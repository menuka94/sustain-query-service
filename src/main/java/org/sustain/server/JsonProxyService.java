package org.sustain.server;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.JsonModelRequest;
import org.sustain.JsonModelResponse;
import org.sustain.JsonProxyGrpc;
import org.sustain.JsonSlidingWindowRequest;
import org.sustain.JsonSlidingWindowResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SlidingWindowRequest;
import org.sustain.SlidingWindowResponse;
import org.sustain.SparkManager;
import org.sustain.SustainGrpc;
import org.sustain.util.Constants;

import java.util.Iterator;

public class JsonProxyService extends JsonProxyGrpc.JsonProxyImplBase {
    private static final Logger log = LogManager.getLogger(JsonProxyService.class);
    private SparkManager sparkManager;

    public JsonProxyService(SparkManager sparkManager) {
        this.sparkManager = sparkManager;
    }

    @Override
    public void modelQuery(JsonModelRequest request,
                           StreamObserver<JsonModelResponse> responseObserver) {
        ManagedChannel channel = null;

        try {
            // open grpc channel
            channel = ManagedChannelBuilder
                .forAddress(Constants.Server.HOST,
                    Constants.Server.PORT)
                .usePlaintext()
                .build();

            // convert json to protobuf and service request
            JsonFormat.Parser parser = JsonFormat.parser();
            JsonFormat.Printer printer = JsonFormat.printer()
                .includingDefaultValueFields()
                .omittingInsignificantWhitespace();

            // create model request
            ModelRequest.Builder requestBuilder =
                ModelRequest.newBuilder();
            parser.merge(request.getJson(), requestBuilder);

            // issue model request
            SustainGrpc.SustainBlockingStub blockingStub =
                SustainGrpc.newBlockingStub(channel);

            Iterator<ModelResponse> iterator =
                blockingStub.modelQuery(requestBuilder.build());

            // iterate over results
            while (iterator.hasNext()) {
                ModelResponse response = iterator.next();

                // build JsonModelRequest
                String json = printer.print(response);
                JsonModelResponse jsonResponse =
                    JsonModelResponse.newBuilder()
                        .setJson(json)
                        .build();

                responseObserver.onNext(jsonResponse);
            }

            // send response
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("failed to evaluate", e);
            responseObserver.onError(e);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    @Override
    public void slidingWindowQuery(JsonSlidingWindowRequest request,
                                   StreamObserver<JsonSlidingWindowResponse> responseObserver) {
        ManagedChannel channel = null;

        try {
            // open grpc channel
            channel = ManagedChannelBuilder
                .forAddress(Constants.Server.HOST,
                    Constants.Server.PORT)
                .usePlaintext()
                .build();

            // convert json to protobuf and service request
            JsonFormat.Parser parser = JsonFormat.parser();
            JsonFormat.Printer printer = JsonFormat.printer()
                .includingDefaultValueFields()
                .omittingInsignificantWhitespace();

            // create model request
            SlidingWindowRequest.Builder requestBuilder =
                SlidingWindowRequest.newBuilder();
            parser.merge(request.getJson(), requestBuilder);

            // issue model request
            SustainGrpc.SustainBlockingStub blockingStub =
                SustainGrpc.newBlockingStub(channel);

            Iterator<SlidingWindowResponse> iterator =
                blockingStub.slidingWindowQuery(requestBuilder.build());

            // iterate over results
            while (iterator.hasNext()) {
                SlidingWindowResponse response = iterator.next();

                // build JsonModelRequest
                String json = printer.print(response);
                JsonSlidingWindowResponse jsonResponse =
                    JsonSlidingWindowResponse.newBuilder()
                        .setJson(json)
                        .build();

                responseObserver.onNext(jsonResponse);
            }

            // send response
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("failed to evaluate", e);
            responseObserver.onError(e);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
