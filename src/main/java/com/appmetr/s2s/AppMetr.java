package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.persister.BatchFactoryServerId;
import com.appmetr.s2s.persister.BatchStorage;
import com.appmetr.s2s.persister.GzippedJsonBatchFactory;
import com.appmetr.s2s.persister.NonBlockingHeapStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.ArrayList;
import java.util.UUID;

public class AppMetr {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    private String token;
    private String url;
    private boolean retryBatchUpload;
    private String serverId = UUID.randomUUID().toString();
    private Clock clock = Clock.systemUTC();
    private BatchStorage batchStorage = new NonBlockingHeapStorage();
    private HttpRequestService httpRequestService = new HttpRequestService();
    private BatchFactoryServerId batchFactory = new GzippedJsonBatchFactory();

    private int maxBatchActions = 1000;
    private long maxBatchBytes = 1024 * 1024;
    private Duration flushPeriod = Duration.ofMinutes(1);

    private boolean stopped = true;
    private long actionsBytes;
    private Instant lastFlushTime = Instant.MAX;
    private ArrayList<Action> actionList = new ArrayList<>();
    private Thread uploadThread;

    public AppMetr() {
    }

    public AppMetr(String token, String url) {
        this.token = token;
        this.url = url;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setRetryBatchUpload(boolean retryBatchUpload) {
        this.retryBatchUpload = retryBatchUpload;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public void setBatchStorage(BatchStorage batchStorage) {
        this.batchStorage = batchStorage;
    }

    public void setHttpRequestService(HttpRequestService httpRequestService) {
        this.httpRequestService = httpRequestService;
    }

    public void setBatchFactory(BatchFactoryServerId batchFactory) {
        this.batchFactory = batchFactory;
    }

    public void setMaxBatchActions(int maxBatchActions) {
        this.maxBatchActions = maxBatchActions;
    }

    public void setMaxBatchBytes(long maxBatchBytes) {
        this.maxBatchBytes = maxBatchBytes;
    }

    public synchronized int getActionsNumber() {
        return actionList.size();
    }

    public synchronized long getActionsBytes() {
        return actionsBytes;
    }

    public synchronized void start() {
        uploadThread = new Thread(this::upload, "appmetr-upload-" + token);
        uploadThread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught upload exception", e));
        uploadThread.start();

        stopped = false;
    }

    /**
     * Does flush and then upload all pending actions.
     * Stops inner threads.
     * May requires some time for competition even no pending actions exist.
     */
    public synchronized void stop() {
        stopped = true;

        try {
            flush();

            uploadThread.interrupt();
            uploadThread.join();

        } catch (InterruptedException e) {
            log.error("AppMetr stopping was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if an Action has been tracked successfully or {@code false} if no space is currently available.
     */
    public synchronized boolean track(Action newAction) throws InterruptedException {
        if (stopped) {
            throw new IllegalStateException("Cannot track because stopped");
        }

        if (needFlush()) {
            if (!flush()) {
                return false;
            }
        }

        actionList.add(newAction);
        actionsBytes += newAction.calcApproximateSize();

        return true;
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if the actions was added to this storage, else
     *         {@code false}
     */
    public synchronized boolean flush() throws InterruptedException {
        log.trace("Flushing started for {} actions", actionList.size());

        if (actionList.isEmpty()) {
            log.debug("Nothing to flush");
            return true;
        }

        final boolean stored = batchStorage.store(actionList, (actions, batchId) -> batchFactory.createBatch(actions, batchId, serverId));
        if (stored) {
            log.debug("Flushing completed for {} actions", actionList.size());
            lastFlushTime = clock.instant();
            actionList.clear();
            actionsBytes = 0;
        } else {
            log.warn("Flushing failed for {} actions", actionList.size());
        }

        return stored;
    }

    public synchronized void flushIfNeeded() throws InterruptedException {
        if (needFlush()) {
            flush();
        }
    }

    private boolean needFlush() {
        return actionsBytes >= maxBatchBytes || actionList.size() >= maxBatchActions || clock.instant().minus(flushPeriod).isAfter(lastFlushTime);
    }

    private void upload() {
        log.trace("Upload starting");

        int uploadedBatchCounter = 0;
        int allBatchCounter = 0;
        long sendBatchesBytes = 0;
        while (true) {
            final Instant batchReadStart = clock.instant();
            final BinaryBatch binaryBatch;
            try {
                binaryBatch = batchStorage.peek();
            } catch (InterruptedException e) {
                break;
            }

            allBatchCounter++;

            log.trace("Batch {} read time: {} ms", binaryBatch.getBatchId(), Duration.between(batchReadStart, clock.instant()));

            final Instant batchUploadStart = clock.instant();
            boolean result;
            try {
                result = httpRequestService.sendRequest(url, token, binaryBatch.getBytes());
            } catch (IOException e) {
                log.error("IOException while sending request", e);
                result = false;
            }

            log.debug("Batch {} {} finished. Took {} ms", binaryBatch.getBatchId(), result ? "" : "NOT", Duration.between(batchUploadStart, clock.instant()));

            if (result) {
                log.trace("Batch {} successfully uploaded", binaryBatch.getBatchId());
                batchStorage.remove();
                uploadedBatchCounter++;
                sendBatchesBytes += binaryBatch.getBytes().length;
            } else {
                log.error("Error while upload batch {}", binaryBatch.getBatchId());
                if (!retryBatchUpload) {
                    batchStorage.remove();
                }
            }

            if (Thread.interrupted()) {
                break;
            }
        }

        log.info("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
        Thread.currentThread().interrupt();
    }
}
