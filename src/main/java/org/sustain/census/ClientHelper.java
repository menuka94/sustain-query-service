package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    public PovertyResponse requestPovertyInfo(String resolution, double latitude, double longitude,
                                              Decade decade) {
        PovertyRequest request = PovertyRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getPoverty(request);
    }

    public TotalPopulationResponse requestTotalPopulation(String resolution, double latitude, double longitude,
                                                          Decade decade) {
        TotalPopulationRequest request =
                TotalPopulationRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getTotalPopulation(request);
    }


    public PopulationByAgeResponse requestPopulationByAge(String resolution, double latitude, double longitude,
                                                          Decade decade) {
        PopulationByAgeRequest request =
                PopulationByAgeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getPopulationByAge(request);
    }

    public MedianHouseholdIncomeResponse requestMedianHouseholdIncome(String resolution, double latitude,
                                                                      double longitude,
                                                                      Decade decade) {
        MedianHouseholdIncomeRequest request =
                MedianHouseholdIncomeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                        .setResolution(resolution)
                        .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                        .setDecade(decade)
                        .build()
                ).build();

        return censusBlockingStub.getMedianHouseholdIncome(request);
    }

    public MedianAgeResponse requestMedianAge(String resolution, double latitude, double longitude,
                                              Decade decade) {
        MedianAgeRequest request = MedianAgeRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getMedianAge(request);
    }

    public RaceResponse requestRace(String resolution, double latitude, double longitude, Decade decade) {
        RaceRequest request = RaceRequest.newBuilder().setSpatialTemporalInfo(SpatialTemporalInfo.newBuilder()
                .setResolution(resolution)
                .setSingleCoordinate(SingleCoordinate.newBuilder().setLatitude(latitude).setLongitude(longitude).build())
                .setDecade(decade)
                .build()
        ).build();

        return censusBlockingStub.getRace(request);
    }

    public TargetedQueryResponse requestTargetedInfo(Predicate.Feature feature, CensusResolution resolution,
                                                     Decade decade,
                                                     Predicate.ComparisonOperator comparisonOperator,
                                                     double comparisonValue) {

        Predicate predicate = Predicate.newBuilder()
                .setComparisonOp(comparisonOperator)
                .setComparisonValue(comparisonValue)
                .setDecade(decade)
                .setFeature(feature)
                .build();

        TargetedQueryRequest request = TargetedQueryRequest.newBuilder()
                .setResolution(resolution)
                .setPredicate(predicate)
                .build();

        return censusBlockingStub.executeTargetedQuery(request);
    }
}
