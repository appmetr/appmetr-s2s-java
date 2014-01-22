package com.appmetr.s2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetrTimer implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(AppMetrTimer.class);

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private volatile Thread pollingThread = null;

    private final long TIMER_PERIOD;

    private final Runnable onTimer;

    public AppMetrTimer(long period, Runnable onTimer) {
        TIMER_PERIOD = period;
        this.onTimer = onTimer;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info("AppMetrTimer started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(TIMER_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("AppMetrTimer - run flusher");
                onTimer.run();
            } catch (InterruptedException ie) {
                logger.warn("Interrupted while polling the queue. Stop polling");

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.error("Error while flushing events", e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("AppMetrTimer stopped!");
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
        logger.info("AppMetrTimer stop triggered!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
