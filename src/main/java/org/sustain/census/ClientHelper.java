package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    PovertyResponse requestPovertyInfo(String resolution, double latitude, double longitude,
                                       Decade decade) {
        PovertyRequest request = PovertyRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getPoverty(request);
    }

    TotalPopulationResponse requestTotalPopulation(String resolution, double latitude, double longitude,
                                                   Decade decade) {
        TotalPopulationRequest request =
                TotalPopulationRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getTotalPopulation(request);
    }


    PopulationByAgeResponse requestPopulationByAge(String resolution, double latitude, double longitude,
                                                   Decade decade) {
        PopulationByAgeRequest request =
                PopulationByAgeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getPopulationByAge(request);
    }

    MedianHouseholdIncomeResponse requestMedianHouseholdIncome(String resolution, double latitude, double longitude,
                                                               Decade decade) {
        MedianHouseholdIncomeRequest request =
                MedianHouseholdIncomeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getMedianHouseholdIncome(request);
    }

    MedianAgeResponse requestMedianAge(String resolution, double latitude, double longitude,
                                       Decade decade) {
        MedianAgeRequest request = MedianAgeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getMedianAge(request);
    }

    RaceResponse requestRace(String resolution, double latitude, double longitude, Decade decade) {
        RaceRequest request = RaceRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getRace(request);
    }

    public TargetQueryResponse requestTargetedInfo(CensusResolution resolution, Decade decade,
                                                   Predicate.ComparisonOperator comparisonOperator,
                                                   double comparisonValue) {

        Predicate predicate = Predicate.newBuilder()
                .setComparisonOp(comparisonOperator)
                .setComparisonValue(comparisonValue)
                .setDecade(decade)
                .build();

        TargetQueryRequest request = TargetQueryRequest.newBuilder()
                .setResolution(resolution)
                .setPredicate(predicate)
                .build();

        return censusBlockingStub.executeTargetedQuery(request);
    }
}
