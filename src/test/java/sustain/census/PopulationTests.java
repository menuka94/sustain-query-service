package sustain.census;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.sustain.census.CensusGrpc;
import org.sustain.census.CensusServer;
import org.sustain.census.ClientHelper;
import org.sustain.census.Constants;
import org.sustain.census.Decade;
import org.sustain.census.TotalPopulationResponse;
import org.sustain.census.db.Util;

import java.io.IOException;
import java.util.ArrayList;

import static org.sustain.census.Constants.CensusResolutions.COUNTY;
import static org.sustain.census.Constants.CensusResolutions.STATE;
import static org.sustain.census.Constants.CensusResolutions.TRACT;

public class PopulationTests {
    private static final Logger log = LogManager.getLogger(PopulationTests.class);
    private static CensusGrpc.CensusBlockingStub censusBlockingStub;
    private static ManagedChannel channel;
    private static CensusServer server;
    private static ClientHelper clientHelper;
    private static ArrayList<Decade> decades;

    private static class ServerRunner extends Thread {
        @Override
        public void run() {
            server = new CensusServer();
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @BeforeAll
    static void init() throws InterruptedException {
        new ServerRunner().start();
        Thread.sleep(2000);
        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
        clientHelper = new ClientHelper(censusBlockingStub);
        decades = new ArrayList<Decade>() {{
            add(Decade._2010);
            add(Decade._2000);
            add(Decade._1990);
            add(Decade._1980);
        }};
    }

    @Test
    void testStatePopulation() {
        for (Decade decade : decades) {
            TotalPopulationResponse populationResponse = clientHelper.requestTotalPopulation(STATE, 24.5, -82, decade);
            Assertions.assertNotNull(populationResponse);
            Assertions.assertTrue(populationResponse.getPopulation() >= 0);
        }
    }

    @Test
    void testCountyPopulation() {
        for (Decade decade : decades) {
            TotalPopulationResponse populationResponse = clientHelper.requestTotalPopulation(COUNTY, 24.5, -82, decade);
            Assertions.assertNotNull(populationResponse);
            Assertions.assertTrue(populationResponse.getPopulation() >= 0);
        }
    }

    @Test
    void testTractPopulation() {
        for (Decade decade : decades) {
            TotalPopulationResponse populationResponse = clientHelper.requestTotalPopulation(TRACT, 24.5, -82, decade);
            Assertions.assertNotNull(populationResponse);
            Assertions.assertTrue(populationResponse.getPopulation() >= 0);
        }
    }

    @AfterAll
    static void cleanUp() {
        server.shutdownNow();
    }
}
