package com.appmetr.s2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetrTimer extends Thread {

    protected static final Logger logger = LoggerFactory.getLogger(AppMetrTimer.class);

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

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
        logger.info(jobName + " started!");

        lock.lock();
        try {
            while (true) {
                try {
                    trigger.await(TIMER_PERIOD, TimeUnit.MILLISECONDS);

                    logger.info(String.format("%s triggered", jobName));

                    onTimer.run();
                } catch (InterruptedException ie) {
                    logger.warn(String.format("Interrupted while waiting. Stop waiting for %s", jobName));
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error(String.format("Error in %s", jobName), e);
                }
            }
        } finally {
            lock.unlock();
        }

        logger.info(String.format("%s stopped!", jobName));
    }

    public void trigger() {
        // there is no need to acquare a lock here, if thread is working - it should pick up newly available job
        if (lock.tryLock()) {
            try {
                trigger.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
