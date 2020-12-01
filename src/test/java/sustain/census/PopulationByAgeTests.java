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
import org.sustain.server.SustainServer;
import org.sustain.census.ClientHelper;
import org.sustain.util.Constants;
import org.sustain.census.Decade;
import org.sustain.census.PopulationByAgeResponse;

import static org.sustain.util.Constants.CensusResolutions.COUNTY;
import static org.sustain.util.Constants.CensusResolutions.STATE;
import static org.sustain.util.Constants.CensusResolutions.TRACT;
import static sustain.census.TestUtil.decades;

@Disabled
public class PopulationByAgeTests {
    private static final Logger log = LogManager.getLogger(PopulationByAgeTests.class);
    private static CensusGrpc.CensusBlockingStub censusBlockingStub;
    private static ManagedChannel channel;
    private static SustainServer server;
    private static ClientHelper clientHelper;

    @BeforeAll
    static void init() throws InterruptedException {
        server = new SustainServer();
        new ServerRunner(server).start();
        Thread.sleep(2000);
        String target = Constants.Server.HOST + ":" + Constants.Server.PORT;
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
        clientHelper = new ClientHelper(censusBlockingStub);
    }

    @AfterAll
    static void cleanUp() {
        server.shutdownNow();
    }

    @Test
    void testStatePopulation() {
        for (Decade decade : decades) {
            PopulationByAgeResponse response = clientHelper.requestPopulationByAge(STATE, 24.5, -82, decade);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get5To9() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get10To14() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get21() >= 0);
        }
        log.info("State Population-by-age tests passed");
    }

    @Test
    void testCountyPopulation() {
        for (Decade decade : decades) {
            PopulationByAgeResponse response = clientHelper.requestPopulationByAge(COUNTY, 24.5, -82, decade);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get5To9() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get10To14() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get21() >= 0);
        }
        log.info("County Population-by-age tests passed");
    }

    @Test
    void testTractPopulation() {
        for (Decade decade : decades) {
            PopulationByAgeResponse response = clientHelper.requestPopulationByAge(TRACT, 24.5, -82, decade);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get5To9() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get10To14() >= 0);
            Assertions.assertTrue(response.getMaleAgeCategories().getAgeCategories().get21() >= 0);
        }
        log.info("Tract Population-by-age tests passed");
    }
}
