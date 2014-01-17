package com.appmetr;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class EventFlusher implements Runnable {

    protected static final Logger logger = Logger.getLogger("EventFlusher");

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private volatile Thread pollingThread = null;

    private static final long FLUSH_PERIOD = DateTimeConstants.MILLIS_PER_MINUTE * 5;

    private final AppMetr appMetr;

    public EventFlusher(AppMetr appMetr){
        //TODO: change AppMetr to Singleton and delete this constructor
        this.appMetr = appMetr;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info("EventFlusher started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(FLUSH_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("EventFlusher - run flusher");
                appMetr.flush();
            } catch (InterruptedException ie) {
                logger.info("Interrupted while polling the queue. Stop polling");

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.info("Error while flushing events: " + e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("EventFlusher stoped!");
    }

    public void trigger() {
        lock.lock();

        try {
            trigger.signal();
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        logger.info("EventFlusher stop triggered!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
