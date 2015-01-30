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
    private final String jobName;

    public AppMetrTimer(long period, Runnable onTimer) {
        this(period, onTimer, "AppMetrTimer");
    }

    public AppMetrTimer(long period, Runnable onTimer, String jobName) {
        TIMER_PERIOD = period;
        this.onTimer = onTimer;
        this.jobName = jobName;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info(jobName + " started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(TIMER_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("%s triggered", jobName);
                onTimer.run();
            } catch (InterruptedException ie) {
                logger.warn("Interrupted while polling the queue. Stop polling for %s", jobName);

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.error(String.format("Error in %s", jobName), e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("%s stopped!", jobName);
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
        logger.info("%s stop triggered!", jobName);

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
