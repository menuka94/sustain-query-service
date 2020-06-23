package org.sustain.census;

import io.grpc.stub.StreamObserver;
import org.sustain.census.controller.mysql.IncomeController;
import org.sustain.census.controller.mysql.PopulationController;
import org.sustain.census.controller.mysql.RaceController;

import java.sql.SQLException;
import java.util.HashMap;

public class ServerHelper {
    public static void executeTargetedPopulationQuery(StreamObserver<TargetedQueryResponse> responseObserver,
                                               String comparisonOp, double comparisonValue, String resolution,
                                               String decade) throws SQLException {
        HashMap<String, String> targetedPopulationResults =
                PopulationController.fetchTargetedInfo(decade,
                        resolution, comparisonOp, comparisonValue);

        TargetedQueryResponse.Builder populationResponseBuilder = TargetedQueryResponse.newBuilder();

        // iterator over results, create SpatialInfo objects, attach to populationResponseBuilder
        for (String key : targetedPopulationResults.keySet()) {
            TargetedQueryResponse.SpatialInfo spatialInfo =
                    TargetedQueryResponse.SpatialInfo.newBuilder()
                            .setGeoId(key)
                            .setName(targetedPopulationResults.get(key))
                            .build();
            populationResponseBuilder.addSpatialInfo(spatialInfo);
        }

        TargetedQueryResponse populationResponse = populationResponseBuilder.build();
        responseObserver.onNext(populationResponse);
        responseObserver.onCompleted();
    }

    public static void executeTargetedIncomeQuery(StreamObserver<TargetedQueryResponse> responseObserver,
                                                  String comparisonOp, double comparisonValue, String resolution,
                                                  String decade) throws SQLException {
        HashMap<String, String> targetedIncomeResults = IncomeController.fetchTargetedInfo(decade,
                resolution, comparisonOp, comparisonValue);

        TargetedQueryResponse.Builder incomeResponseBuilder = TargetedQueryResponse.newBuilder();

        // iterator over results, create SpatialInfo objects, attach incomeResponseBuilder
        for (String key : targetedIncomeResults.keySet()) {
            TargetedQueryResponse.SpatialInfo spatialInfo =
                    TargetedQueryResponse.SpatialInfo.newBuilder()
                            .setGeoId(key)
                            .setName(targetedIncomeResults.get(key))
                            .build();
            incomeResponseBuilder.addSpatialInfo(spatialInfo);
        }

        TargetedQueryResponse incomeResponse = incomeResponseBuilder.build();
        responseObserver.onNext(incomeResponse);
        responseObserver.onCompleted();
    }

    public static void executeTargetedRaceQuery(StreamObserver<TargetedQueryResponse> responseObserver,
                                                String comparisonField, String comparisonOp, double comparisonValue,
                                                String resolution, String decade) throws SQLException {
        HashMap<String, String> targetedRaceResults = RaceController.fetchTargetedInfo(decade,
                resolution, comparisonField, comparisonOp, comparisonValue);

        TargetedQueryResponse.Builder raceResponseBuilder = TargetedQueryResponse.newBuilder();

        // iterator over results, create SpatialInfo objects, attach to raceResponseBuilder
        for (String key : targetedRaceResults.keySet()) {
            TargetedQueryResponse.SpatialInfo spatialInfo =
                    TargetedQueryResponse.SpatialInfo.newBuilder()
                            .setGeoId(key)
                            .setName(targetedRaceResults.get(key))
                            .build();
            raceResponseBuilder.addSpatialInfo(spatialInfo);
        }

        TargetedQueryResponse incomeResponse = raceResponseBuilder.build();
        responseObserver.onNext(incomeResponse);
        responseObserver.onCompleted();
    }
}
