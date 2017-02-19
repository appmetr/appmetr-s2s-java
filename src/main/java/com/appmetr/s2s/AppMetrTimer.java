package com.appmetr.s2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetrTimer extends Thread {

    protected static final Logger log = LoggerFactory.getLogger(AppMetrTimer.class);

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private final long timerPeriod;
    private final Runnable onTimer;
    private final String jobName;
    private volatile boolean stopped;

    public AppMetrTimer(long period, Runnable onTimer) {
        this(period, onTimer, "AppMetrTimer");
    }

    public AppMetrTimer(long period, Runnable onTimer, String jobName) {
        timerPeriod = period;
        this.onTimer = onTimer;
        this.jobName = jobName;
    }

    @Override public void run() {
        log.info("{} started!", jobName);

        lock.lock();
        try {
            while (!stopped) {
                try {
                    trigger.await(timerPeriod, TimeUnit.MILLISECONDS);

                    log.info("{} triggered", jobName);

                    onTimer.run();
                } catch (InterruptedException ie) {
                    log.warn("Interrupted while waiting. Stop waiting for {}", jobName);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in {}", jobName, e);
                }
            }
        } finally {
            lock.unlock();
        }

        log.info("{} stopped!", jobName);
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

    public void triggerAndStop() {
        stopped = true;
        trigger();
    }
}
