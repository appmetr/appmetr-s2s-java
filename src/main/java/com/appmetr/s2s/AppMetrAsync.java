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

        if (appMetr.isStopped()) {
            appMetr.start();
        }

        periodicFlushingFuture = executorService.scheduleWithFixedDelay(() -> {
            try {
                appMetr.flushIfNeeded();
            } catch (Throwable t) {
                log.error("Periodic flushing failed", t);
            }
        }, appMetr.flushPeriod.toMillis(), appMetr.flushPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Boolean> track(Action newAction) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return appMetr.track(newAction);
            } catch (Throwable t) {
                log.error("Track {} failed", newAction, t);
                throw new RuntimeException(t);
            }
        }, executorService);
    }

    public CompletableFuture<Void> stop() {
        return CompletableFuture.runAsync(() -> {
            periodicFlushingFuture.cancel(false);
            executorService.shutdown();
        }, executorService).thenCompose(aVoid -> CompletableFuture.runAsync(() -> {
            try {
                awaitTermination();
                appMetr.softStop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, r -> new Thread(r, "appmetr-async-stop").start()));
    }

    protected void awaitTermination() throws InterruptedException {
        while (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
            log.warn("ExecutorService {} do not terminated", executorService);
        }
    }
}
