package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Should be used with caution. Could produce OOM error!
 */
public class AppMetrAsync {
    private static final Logger log = LoggerFactory.getLogger(AppMetrAsync.class);

    private final AppMetr appMetr;
    private final ScheduledExecutorService executorService;

    private ScheduledFuture<?> periodicFlushingFuture;

    public AppMetrAsync(AppMetr appMetr) {
        this(appMetr, Executors.newScheduledThreadPool(1, r -> new Thread(r, "appmetr-async")));
    }

    public AppMetrAsync(AppMetr appMetr, ScheduledExecutorService executorService) {
        this.appMetr = appMetr;
        this.executorService = executorService;

        appMetr.start();
        periodicFlushingFuture = executorService.scheduleWithFixedDelay(() -> {
            try {
                appMetr.flushIfNeeded();
            } catch (Throwable t) {
                log.error("Periodic flushing failed", t);
            }
        }, appMetr.flushPeriod.toMillis(), appMetr.flushPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> track(Action newAction) {
        return CompletableFuture.runAsync(() -> {
            try {
                appMetr.track(newAction);
            } catch (Throwable t) {
                log.error("Track {} failed", newAction, t);
            }
        }, executorService);
    }

    public void stop() {
        periodicFlushingFuture.cancel(false);
        executorService.shutdown();
        awaitTermination();
        appMetr.stop();
    }

    protected void awaitTermination() {
        try {
            while (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("ExecutorService {} do not terminated", executorService);
            }
        } catch (InterruptedException e) {
            log.warn("Executor Service was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
