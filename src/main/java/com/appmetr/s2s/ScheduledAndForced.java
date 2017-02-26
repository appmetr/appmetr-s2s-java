package com.appmetr.s2s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

public class ScheduledAndForced {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    private final ScheduledExecutorService executor;
    private final Runnable runnable;
    private final BooleanSupplier predicate;
    private final long period;
    private final long rescheduleDelta;

    private final Lock taskLock = new ReentrantLock();
    private final Lock futureLock = new ReentrantLock();
    private final Condition futureCondition = futureLock.newCondition();
    private volatile Future<?> scheduledFuture;
    private Future<?> forcedFuture;
    private long finishedTime;
    private boolean stopped;

    public ScheduledAndForced(ScheduledExecutorService executor, Runnable runnable, BooleanSupplier predicate, long initialDelay, long period) {
        this.executor = executor;
        this.runnable = runnable;
        this.predicate = predicate;
        this.period = period;
        this.rescheduleDelta = period / 5;

        scheduledFuture = executor.schedule(scheduleWrapped(), initialDelay, TimeUnit.MILLISECONDS);
    }

    public ScheduledAndForced(ScheduledExecutorService executor, Runnable runnable, long initialDelay, long period) {
        this(executor, runnable, () -> true, initialDelay, period);
    }

    public ScheduledAndForced(ScheduledExecutorService executor, Runnable runnable, long period) {
        this(executor, runnable, period, period);
    }

    public synchronized void force() {
        checkStopped();

        futureLock.lock();
        try {
            if (forcedFuture == null) {
                forcedFuture = executor.submit(forceWrapped());
            }
        } finally {
            futureLock.unlock();
        }
    }

    public synchronized void stop() throws InterruptedException {
        checkStopped();
        stopped = true;

        try {
            if (!scheduledFuture.cancel(false)) {
                scheduledFuture.get();
                scheduledFuture.cancel(false);
            }

            futureLock.lock();
            try {
                while (forcedFuture != null) {
                    futureCondition.await();
                }
            } finally {
                futureLock.unlock();
            }

        } catch (ExecutionException e) {
            log.error("Exception while execution", e);
        }
    }

    protected Runnable scheduleWrapped() {
        return new Runnable() {
            @Override public void run() {
                if (taskLock.tryLock()) {
                    try {
                        final long startTime = System.currentTimeMillis();
                        final long timePassed = startTime - finishedTime;
                        if (timePassed < period - rescheduleDelta) {
                            scheduledFuture = executor.schedule(this, period - timePassed, TimeUnit.MILLISECONDS);
                            return;
                        }

                        try {
                            if (!predicate.getAsBoolean()) {
                                return;
                            }

                            runnable.run();
                        } catch (Exception e) {
                            log.error("Exception during execution", e);
                        } finally {
                            scheduledFuture = executor.schedule(runnable, period, TimeUnit.MILLISECONDS);
                            finishedTime = System.currentTimeMillis();
                        }
                    } finally {
                        taskLock.unlock();
                    }
                } else {
                    scheduledFuture = executor.schedule(runnable, period, TimeUnit.MILLISECONDS);
                }
            }
        };
    }

    protected Runnable forceWrapped() {
        return () -> {
            try {
                if (taskLock.tryLock()) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        log.error("Exception during execution", e);
                    } finally {
                        finishedTime = System.currentTimeMillis();
                        taskLock.unlock();
                    }
                }
            } finally {
                futureLock.lock();
                try {
                    forcedFuture = null;
                    futureCondition.signal();
                } finally {
                    futureLock.unlock();
                }
            }
        };
    }

    protected void checkStopped() {
        if (stopped) {
            throw new IllegalStateException("Already stopped");
        }
    }
}
