package sustain.census;

import org.sustain.census.CensusServer;

import java.io.IOException;

public class ServerRunner extends Thread {

    private CensusServer server;

    public ServerRunner(CensusServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        try {
            server.start();
            server.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
