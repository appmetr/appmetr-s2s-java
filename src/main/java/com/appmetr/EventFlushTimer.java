package com.appmetr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EventFlushTimer implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger("EventFlushTimer");

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private volatile Thread pollingThread = null;

    private static final long FLUSH_PERIOD = DateTimeConstants.MILLIS_PER_MINUTE * 3;//TODO: change that

    private final AppMetr appMetr;

    public EventFlushTimer(AppMetr appMetr){
        //TODO: change AppMetr to Singleton and delete this constructor
        this.appMetr = appMetr;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info("EventFlushTimer started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(FLUSH_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("EventFlushTimer - run flusher");
                appMetr.flush();
            } catch (InterruptedException ie) {
                logger.warn("Interrupted while polling the queue. Stop polling");

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.error("Error while flushing events", e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("EventFlushTimer stopped!");
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
        logger.info("EventFlushTimer stop triggered!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
