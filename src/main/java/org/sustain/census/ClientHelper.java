package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    PovertyResponse requestPovertyInfo(String resolution, double latitude, double longitude) {
        PovertyRequest request = PovertyRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build()
        ).build();

        return censusBlockingStub.getPoverty(request);
    }

    TotalPopulationResponse requestTotalPopulation(String resolution, double latitude, double longitude) {
        TotalPopulationRequest request =
                TotalPopulationRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .build()
                ).build();

        return censusBlockingStub.getTotalPopulation(request);
    }


    PopulationByAgeResponse requestPopulationByAge(String resolution, double latitude, double longitude) {
        PopulationByAgeRequest request =
                PopulationByAgeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .build()
                ).build();

        return censusBlockingStub.getPopulationByAge(request);
    }

    MedianHouseholdIncomeResponse requestMedianHouseholdIncome(String resolution, double latitude,
                                                               double longitude) {
        MedianHouseholdIncomeRequest request =
                MedianHouseholdIncomeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude)
                        .build()
                ).build();

        return censusBlockingStub.getMedianHouseholdIncome(request);
    }

    MedianAgeResponse requestMedianAge(String resolution, double latitude, double longitude) {
        MedianAgeRequest request = MedianAgeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build()
        ).build();

        return censusBlockingStub.getMedianAge(request);
    }

    RaceResponse requestRace(String resolution, double latitude, double longitude) {
        RaceRequest request = RaceRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                .setResolution(resolution)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build()
        ).build();

        return censusBlockingStub.getRace(request);
    }
}
