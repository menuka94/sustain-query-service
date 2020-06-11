package org.sustain.census;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CensusClient {
    private static final Logger log = LogManager.getLogger(CensusClient.class);
    private final CensusGrpc.CensusBlockingStub censusBlockingStub;
    private final ClientHelper clientHelper;

    public CensusClient(Channel channel) {
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
        clientHelper = new ClientHelper(censusBlockingStub);
    }

    public static void main(String[] args) {
        //if (args.length != 1) {
        //    log.warn("Enter resolution. Example: CensusClient <state/county/tract>");
        //    System.exit(0);
        //}
        String resolution = "state";
        double latitude = 24.5;
        double longitude = -82;

        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        SpatialTemporalInfo spatialTemporalInfo = SpatialTemporalInfo.newBuilder()
                .setResolution(Constants.CensusResolutions.TRACT)
                .setDecade(Decade._2010)
                .setBoundingBox(BoundingBox.newBuilder()
                        .setX1(40.5)
                        .setY1(-105.1)
                        .setX2(40.6)
                        .setY2(-105.0)
                        .build())
                .build();
        try {
            CensusClient client = new CensusClient(channel);

            MedianHouseholdIncomeRequest request = MedianHouseholdIncomeRequest.newBuilder()
                    .setSpatialTemporalInfo(spatialTemporalInfo)
                    .build();

            MedianHouseholdIncomeResponse incomeResponse =
                    client.censusBlockingStub.getMedianHouseholdIncome(request);
            log.info("Income: " + incomeResponse.getMedianHouseholdIncome());

            TotalPopulationRequest request1 = TotalPopulationRequest.newBuilder()
                    .setSpatialTemporalInfo(spatialTemporalInfo)
                    .build();
            TotalPopulationResponse populationResponse =
                    client.censusBlockingStub.getTotalPopulation(request1);
            log.info("Population: " + populationResponse.getPopulation());

            System.exit(0);

            // Total Population
            TotalPopulationResponse totalPopulation = client.clientHelper.requestTotalPopulation(resolution, latitude
                    , longitude, Decade._2010);
            log.info("Total Population:" + totalPopulation.getPopulation());

            // PopulationByAge
            PopulationByAgeResponse populationByAge =
                    client.clientHelper.requestPopulationByAge(Constants.CensusResolutions.STATE, latitude, longitude
                            , Decade._2010);
            AgeCategories malePopulation = populationByAge.getMaleAgeCategories().getAgeCategories();
            System.out.println();
            log.info("Male Population by age categories:");
            log.info(malePopulation.getTotal());
            log.info(malePopulation.getUnder5());
            log.info(malePopulation.get5To9());
            log.info(malePopulation.get10To14());
            log.info(malePopulation.get15To17());
            log.info(malePopulation.get18To19());
            log.info(malePopulation.get20());
            log.info(malePopulation.get21());
            log.info(malePopulation.get22To24());
            log.info(malePopulation.get25To29());
            log.info(malePopulation.get30To34());
            log.info(malePopulation.get35To39());
            log.info(malePopulation.get40To44());
            log.info(malePopulation.get45To49());
            log.info(malePopulation.get50To54());
            log.info(malePopulation.get55To59());
            log.info(malePopulation.get60To61());
            log.info(malePopulation.get62To64());
            log.info(malePopulation.get65To66());
            log.info(malePopulation.get67To69());
            log.info(malePopulation.get70To74());
            log.info(malePopulation.get75To79());
            log.info(malePopulation.get80To84());
            log.info(malePopulation.get85AndOver());

            AgeCategories femalePopulation = populationByAge.getFemaleAgeCategories().getAgeCategories();
            System.out.println();
            log.info("Female population by age categories");
            log.info(femalePopulation.getTotal());
            log.info(femalePopulation.getUnder5());
            log.info(femalePopulation.get5To9());
            log.info(femalePopulation.get10To14());
            log.info(femalePopulation.get15To17());
            log.info(femalePopulation.get18To19());
            log.info(femalePopulation.get20());
            log.info(femalePopulation.get21());
            log.info(femalePopulation.get22To24());
            log.info(femalePopulation.get25To29());
            log.info(femalePopulation.get30To34());
            log.info(femalePopulation.get35To39());
            log.info(femalePopulation.get40To44());
            log.info(femalePopulation.get45To49());
            log.info(femalePopulation.get50To54());
            log.info(femalePopulation.get55To59());
            log.info(femalePopulation.get60To61());
            log.info(femalePopulation.get62To64());
            log.info(femalePopulation.get65To66());
            log.info(femalePopulation.get67To69());
            log.info(femalePopulation.get70To74());
            log.info(femalePopulation.get75To79());
            log.info(femalePopulation.get80To84());
            log.info(femalePopulation.get85AndOver());
            System.out.println();

            // MedianHouseholdIncome
            MedianHouseholdIncomeResponse medianHouseholdIncome =
                    client.clientHelper.requestMedianHouseholdIncome(resolution,
                            latitude, longitude, Decade._2010);
            log.info("Median Household Income: " + medianHouseholdIncome.getMedianHouseholdIncome());
            System.out.println();

            // MedianAge
            MedianAgeResponse medianAge = client.clientHelper.requestMedianAge(resolution, latitude, longitude,
                    Decade._2010);
            log.info("Median Age: " + medianAge.getMedianAge());
            System.out.println();

            // Poverty
            PovertyResponse poverty = client.clientHelper.requestPovertyInfo("state", latitude, longitude,
                    Decade._2010);
            log.info("Income level below poverty level: " + poverty.getIncomeBelowPovertyLevel());
            log.info("Income level at or above poverty level: " + poverty.getIncomeAtOrAbovePovertyLevel());
            System.out.println();

            // Targeted Query - 1
            // retrieve all states where population is greater than 10 million
            TargetedQueryResponse populationTargetedQueryResponse =
                    client.clientHelper.requestTargetedInfo(Predicate.Feature.Population, CensusResolution.County,
                            Decade._2010, Predicate.ComparisonOperator.GREATER_THAN, 1000000);
            List<TargetedQueryResponse.SpatialInfo> spatialInfoListPopulation =
                    populationTargetedQueryResponse.getSpatialInfoList();
            log.info("Counties where population is greater than 1 million");
            for (TargetedQueryResponse.SpatialInfo spatialInfo : spatialInfoListPopulation) {
                log.info(spatialInfo.getGeoId() + ": " + spatialInfo.getName());
            }
            System.out.println();

            // Targeted Query - 2
            // retrieve all counties where median household income is less than or equal to $20,000/year
            TargetedQueryResponse incomeTargetedQueryResponse =
                    client.clientHelper.requestTargetedInfo(Predicate.Feature.Income, CensusResolution.County,
                            Decade._2010, Predicate.ComparisonOperator.LESS_THAN_OR_EQUAL, 20000);
            List<TargetedQueryResponse.SpatialInfo> spatialInfoListIncome =
                    incomeTargetedQueryResponse.getSpatialInfoList();
            log.info("Counties where median household income is less than or equal to $20,000/year");
            for (TargetedQueryResponse.SpatialInfo spatialInfo : spatialInfoListIncome) {
                log.info(spatialInfo.getGeoId() + ": " + spatialInfo.getName());
            }

        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
