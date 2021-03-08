import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.server.SustainServer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class InProcessServer<T extends io.grpc.BindableService> {

    private static final Logger log = LogManager.getLogger(SustainServer.class);

    private Server server;

    private Class<T> clazz;

    public InProcessServer(Class<T> clazz){
        this.clazz = clazz;
    }

    public void start() throws IOException, InstantiationException, IllegalAccessException {

        try {
            server = InProcessServerBuilder
                    .forName("test")
                    .directExecutor()
                    .addService(clazz.getDeclaredConstructor().newInstance())
                    .build()
                    .start();
            log.info("InProcessServer started.");
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    InProcessServer.this.stop();
                    System.err.println("*** server shut down");
                }
            });
        } catch (NoSuchMethodException e) {
            log.error("Caught NoSuchMethodException: ", e.getCause());
        } catch (InvocationTargetException e) {
            log.error("Caught InvocationTargetException: ", e.getCause());
        }
    }

    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
