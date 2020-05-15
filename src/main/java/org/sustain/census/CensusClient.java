package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            log.warn("Enter resolution. Example: CensusClient state, or CensusClient county");
            System.exit(0);
        }
        String resolution = args[0];
        double latitude = 24.5;
        double longitude = -82;

        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        try {
            CensusClient client = new CensusClient(channel);

            // Total Population
            TotalPopulationResponse totalPopulation = client.requestTotalPopulation(resolution, latitude, longitude);
            log.info("Total Population:" + totalPopulation.getPopulation());

            // PopulationByAge
//            PopulationByAgeResponse populationByAge = client.requestPopulationByAge(resolution, latitude, longitude);
//            AgeCategories malePopulation = populationByAge.getMaleAgeCategories().getAgeCategories();
//            log.info(malePopulation.getTotal());
//            log.info(malePopulation.getUnder5());
//            log.info(malePopulation.get5To9());
//            log.info(malePopulation.get10To14());
//            log.info(malePopulation.get15To17());
//
//            AgeCategories femalePopulation = populationByAge.getFemaleAgeCategories().getAgeCategories();
//            log.info(femalePopulation.getTotal());
//            log.info(femalePopulation.getUnder5());
//            log.info(femalePopulation.get5To9());
//            log.info(femalePopulation.get10To14());
//            log.info(femalePopulation.get15To17());

            // MedianHouseholdIncome
            MedianHouseholdIncomeResponse medianHouseholdIncome = client.requestMedianHouseholdIncome(resolution,
                    latitude, longitude);
            log.info("Median Household Income: " + medianHouseholdIncome.getMedianHouseholdIncome());

            // MedianAge
            MedianAgeResponse medianAge = client.requestMedianAge(resolution, latitude, longitude);
            log.info("Median Age: " + medianAge.getMedianAge());
        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private TotalPopulationResponse requestTotalPopulation(String resolution, double latitude, double longitude) {
        TotalPopulationRequest request =
                TotalPopulationRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude).build()
                ).build();

        return censusBlockingStub.getTotalPopulation(request);
    }


    private PopulationByAgeResponse requestPopulationByAge(String resolution, double latitude, double longitude) {
        PopulationByAgeRequest request =
                PopulationByAgeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude).build()
                ).build();

        return censusBlockingStub.getPopulationByAge(request);
    }

    private MedianHouseholdIncomeResponse requestMedianHouseholdIncome(String resolution, double latitude,
                                                                       double longitude) {
        MedianHouseholdIncomeRequest request =
                MedianHouseholdIncomeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude).build()
                ).build();

        return censusBlockingStub.getMedianHouseholdIncome(request);
    }

    private MedianAgeResponse requestMedianAge(String resolution, double latitude, double longitude) {
        MedianAgeRequest request =
                MedianAgeRequest.newBuilder().setSpatialInfo(SpatialInfoInRequest.newBuilder()
                        .setResolution(resolution)
                        .setLatitude(latitude)
                        .setLongitude(longitude).build()
                ).build();

        return censusBlockingStub.getMedianAge(request);
    }
}
