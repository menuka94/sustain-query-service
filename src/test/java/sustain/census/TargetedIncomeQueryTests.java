package sustain.census;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sustain.census.CensusGrpc;
import org.sustain.census.CensusResolution;
import org.sustain.server.SustainServer;
import org.sustain.census.ClientHelper;
import org.sustain.util.Constants;
import org.sustain.census.Decade;
import org.sustain.census.Predicate;
import org.sustain.census.TargetedQueryResponse;
import org.sustain.db.Util;

import java.util.List;

import static sustain.census.TestUtil.decades;

@Disabled
public class TargetedIncomeQueryTests {
    private static final Logger log = LogManager.getLogger(TargetedIncomeQueryTests.class);

    private static CensusGrpc.CensusBlockingStub censusBlockingStub;
    private static ManagedChannel channel;
    private static SustainServer server;
    private static ClientHelper clientHelper;

    @BeforeAll
    static void init() throws InterruptedException {
        server = new SustainServer();
        new ServerRunner(server).start();
        Thread.sleep(2000);
        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
        clientHelper = new ClientHelper(censusBlockingStub);
    }

    @AfterAll
    static void cleanUp() {
        server.shutdownNow();
    }

    @Test
    public void testStateIncomeTargeted() {
        for (Decade decade : decades) {
            TargetedQueryResponse targetedQueryResponse = clientHelper.requestTargetedInfo(
                    Predicate.Feature.Income,
                    CensusResolution.State,
                    decade,
                    Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                    50000);
            log.info("Tests: States where median household income is greater than $50,000 in  " + decade.toString());
            Assertions.assertNotNull(targetedQueryResponse);

            List<TargetedQueryResponse.SpatialInfo> spatialInfoList = targetedQueryResponse.getSpatialInfoList();

            for (TargetedQueryResponse.SpatialInfo spatialInfo : spatialInfoList) {
                Assertions.assertNotNull(spatialInfo);
                Assertions.assertNotEquals("", spatialInfo.getGeoId());
                Assertions.assertNotEquals("", spatialInfo.getName());
            }
        }
    }

    @Test
    public void testCountyIncomeTargeted() {
        for (Decade decade : decades) {
            TargetedQueryResponse targetedQueryResponse = clientHelper.requestTargetedInfo(
                    Predicate.Feature.Income,
                    CensusResolution.County,
                    decade,
                    Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                    50000);
            log.info("Tests: Counties where median household income is greater than $50,000 in  " + decade.toString());
            Assertions.assertNotNull(targetedQueryResponse);

            List<TargetedQueryResponse.SpatialInfo> spatialInfoList = targetedQueryResponse.getSpatialInfoList();

            for (TargetedQueryResponse.SpatialInfo spatialInfo : spatialInfoList) {
                Assertions.assertNotNull(spatialInfo);
                Assertions.assertNotEquals("", spatialInfo.getGeoId());
                Assertions.assertNotEquals("", spatialInfo.getName());
            }
        }
    }


    @Test
    public void testTractIncomeTargeted() {
        for (Decade decade : decades) {
            TargetedQueryResponse targetedQueryResponse = clientHelper.requestTargetedInfo(
                    Predicate.Feature.Income,
                    CensusResolution.Tract,
                    decade,
                    Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                    50000);
            log.info("Tests: Tracts where median household income is greater than $50,000 in  " + decade.toString());
            Assertions.assertNotNull(targetedQueryResponse);

            List<TargetedQueryResponse.SpatialInfo> spatialInfoList = targetedQueryResponse.getSpatialInfoList();

            for (TargetedQueryResponse.SpatialInfo spatialInfo : spatialInfoList) {
                Assertions.assertNotNull(spatialInfo);
                Assertions.assertNotEquals("", spatialInfo.getGeoId());
                Assertions.assertNotEquals("", spatialInfo.getName());
            }
        }
    }
}
