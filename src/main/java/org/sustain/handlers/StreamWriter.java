package org.sustain.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class StreamWriter<T, R> {

    private static final Logger log = LogManager.getLogger(StreamWriter.class);

    protected AtomicLong activeCount;
    protected LinkedBlockingQueue<T> queue;
    protected List<Thread> threads;

    public StreamWriter(StreamObserver<R> responseObserver,
                        int threadCount) {
        this.activeCount = new AtomicLong(0);
        this.queue = new LinkedBlockingQueue<T>();
        this.threads = new ArrayList<Thread>();

        // initialize threads
        ReentrantLock lock = new ReentrantLock();
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {

                try {
                    while (true) {
                        // retrieve next document
                        T t = queue.take();

                        // initialize response
                        R r = convert(t);

                        // send response
                        lock.lock();
                        try {
                            responseObserver.onNext(r);
                        } finally {
                            lock.unlock();
                        }

                        // decrement active count
                        activeCount.decrementAndGet();
                    }
                } catch (InterruptedException e) {
                    log.info("Caught InterruptedException: " + e.getMessage());
                }
            });

            this.threads.add(thread);
        }
    }

    public void add(T t) throws Exception {
        // add datum to processing queue and increment active count
        this.queue.add(t);
        this.activeCount.incrementAndGet();
    }

    public abstract R convert(T t);

    public void start() throws Exception {
        // start all threads
        for (Thread thread : this.threads) {
            thread.start();
        }
    }

    public void stop(boolean force) throws Exception {
        // wait for all active items to complete
        if (!force) {
            while (this.activeCount.get() != 0) {
                Thread.sleep(50);
            }
        }

        // stop all threads
        for (Thread thread : this.threads) {
            thread.interrupt();
        }
    }

    public void waitForCompletion() throws Exception {
        // wait for all active items to complete
        while (this.activeCount.get() != 0) {
            Thread.sleep(50);
        }
    }
}
