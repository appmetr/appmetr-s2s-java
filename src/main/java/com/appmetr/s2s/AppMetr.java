package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.persister.*;
import com.appmetr.s2s.sender.HttpBatchSender;
import com.appmetr.s2s.sender.BatchSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.ArrayList;
import java.util.UUID;

public class AppMetr {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    protected String token;
    protected String url;
    protected boolean retryBatchUpload = true;
    protected String serverId = UUID.randomUUID().toString();
    protected Clock clock = Clock.systemUTC();
    protected BatchStorage batchStorage = new HeapStorage(HeapStorage.DEFAULT_MAX_BYTES);
    protected BatchSender batchSender = new HttpBatchSender();
    protected BatchFactoryServerId batchFactory = GzippedJsonBatchFactory.instance;

    protected int maxBatchActions = 1000;
    protected long maxBatchBytes = 1024 * 1024;
    protected Duration flushPeriod = Duration.ofMinutes(1);
    protected Duration readRetryTimeout = Duration.ofSeconds(3);
    protected Duration failedUploadTimeout = Duration.ofSeconds(1);

    protected volatile boolean stopped = true;
    protected volatile boolean hardStop;
    protected volatile boolean softStop;
    protected long actionsBytes;
    protected Instant lastFlushTime;
    protected ArrayList<Action> actionList = new ArrayList<>();
    protected Thread uploadThread;
    protected volatile Throwable lastUploadThrowable;

    protected AppMetr() {
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

    public void setBatchSender(BatchSender batchSender) {
        this.batchSender = batchSender;
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

    public void setFlushPeriod(Duration flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    public void setReadRetryTimeout(Duration readRetryTimeout) {
        this.readRetryTimeout = readRetryTimeout;
    }

    public void setFailedUploadTimeout(Duration failedUploadTimeout) {
        this.failedUploadTimeout = failedUploadTimeout;
    }

    public boolean isStopped() {
        return stopped;
    }

    /**
     * Starts uploading
     */
    public synchronized void start() {
        if (!stopped) {
            throw new IllegalStateException("Appmetr is in running state");
        }

        uploadThread = new Thread(this::upload, "appmetr-upload-" + token);
        uploadThread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught upload exception", e));
        uploadThread.start();

        lastFlushTime = clock.instant();
        lastUploadThrowable = null;
        stopped = false;
    }

    /**
     * Does flush and stop uploading if backing storage is persistent or no batches it it
     */
    public synchronized void stop() {
        stopped = true;
        try {
            flush();
            batchStorage.shutdown();

            uploadThread.interrupt();
            uploadThread.join();

        } catch (InterruptedException e) {
            log.error("AppMetr stopping was interrupted", e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.error("Exception while stopping", e);
        }
    }

    /**
     * Stopping without waiting for upload any batches
     */
    public void hardStop() {
        hardStop = true;
        stop();
    }

    /**
     * Waiting until upload all batches then stop
     */
    public void softStop() {
        softStop = true;
        stop();
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if an Action has been tracked successfully or {@code false} if no space is currently available.
     */
    public synchronized boolean track(Action newAction) throws InterruptedException, IOException {
        checkState();

        if (needFlush()) {
            if (!flush()) {
                return false;
            }
        }

        actionList.add(newAction);
        actionsBytes += newAction.calcApproximateSize();

        return true;
    }

    protected void checkState() {
        if (stopped) {
            throw new IllegalStateException("Appmetr is in stopped state", lastUploadThrowable);
        }
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if the actions was added to this storage, else
     *         {@code false}
     */
    public synchronized boolean flush() throws InterruptedException, IOException {
        log.trace("Flushing {} actions", actionList.size());

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

    public synchronized boolean flushIfNeeded() throws InterruptedException, IOException {
        checkState();
        
        if (needFlush()) {
            flush();
            return true;
        }

        return false;
    }

    public Throwable getLastUploadError() {
        return lastUploadThrowable;
    }

    protected boolean needFlush() {
        return actionsBytes >= maxBatchBytes
                || (maxBatchActions > 0 && actionList.size() >= maxBatchActions)
                ||  !clock.instant().minus(flushPeriod).isBefore(lastFlushTime);
    }

    protected void upload() {
        log.trace("Upload starting");

        int uploadedBatchCounter = 0;
        int allBatchCounter = 0;
        long sendBatchesBytes = 0;
        while (true) {
            final Instant batchReadStart = clock.instant();
            final BinaryBatch binaryBatch;
            try {
                binaryBatch = batchStorage.get();
            } catch (InterruptedException e) {
                if (shouldInterrupt()) {
                    break;
                } else {
                    continue;
                }
            } catch (IOException e) {
                log.error("Error while reading batch", e);
                try {
                    Thread.sleep(readRetryTimeout.toMillis());
                    continue;
                } catch (InterruptedException ie) {
                    if (shouldInterrupt()) {
                        Thread.currentThread().interrupt();
                        break;
                    } else {
                        continue;
                    }
                }
            }

            allBatchCounter++;

            log.trace("Batch {} read time: {}", binaryBatch.getBatchId(), Duration.between(batchReadStart, clock.instant()));

            while (true) {
                final Instant batchUploadStart = clock.instant();
                boolean result;
                try {
                    result = batchSender.send(url, token, binaryBatch.getBytes());
                } catch (Throwable e) {
                    log.error("Unexpected exception while sending the batch {}", binaryBatch.getBatchId(), e);
                    lastUploadThrowable = e;
                    stopped = true;
                    return;
                }

                log.debug("Batch {} {} finished. Took {}", binaryBatch.getBatchId(), result ? "" : "NOT", Duration.between(batchUploadStart, clock.instant()));

                if (result) {
                    log.trace("Batch {} successfully uploaded", binaryBatch.getBatchId());
                    tryRemove(binaryBatch.getBatchId());
                    uploadedBatchCounter++;
                    sendBatchesBytes += binaryBatch.getBytes().length;
                    break;
                }

                log.error("Error while uploading batch {}", binaryBatch.getBatchId());

                try {
                    Thread.sleep(failedUploadTimeout.toMillis());
                } catch (InterruptedException e) {
                    if (shouldInterrupt()) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (!retryBatchUpload) {
                    tryRemove(binaryBatch.getBatchId());
                    break;
                }

                log.info("Retrying the batch {}", binaryBatch.getBatchId());
            }

            if (stopped && shouldInterrupt()) {
                break;
            }
        }

        log.info("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
    }

    protected void tryRemove(long batchId) {
        try {
            batchStorage.remove();
        } catch (IOException e) {
            log.error("Error while removing uploaded batch {}", batchId, e);
        }
    }

    protected boolean shouldInterrupt() {
        return hardStop || (batchStorage.isPersistent() && !softStop) || batchStorage.isEmpty();
    }

    public static long getTimeKey() {
        return Action.createTimeKey(System.currentTimeMillis());
    }
}
