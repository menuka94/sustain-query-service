package sustain.census;

import org.sustain.server.SustainServer;

import java.io.IOException;

public class ServerRunner extends Thread {

    private SustainServer server;

    public ServerRunner(SustainServer server) {
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
