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
    protected boolean retryBatchUpload;
    protected String serverId = UUID.randomUUID().toString();
    protected Clock clock = Clock.systemUTC();
    protected BatchStorage batchStorage = new HeapStorage();
    protected BatchSender batchSender = new HttpBatchSender();
    protected BatchFactoryServerId batchFactory = GzippedJsonBatchFactory.instance;

    protected int maxBatchActions = 1000;
    protected long maxBatchBytes = 1024 * 1024;
    protected Duration flushPeriod = Duration.ofMinutes(1);
    protected Duration readRetryTimeout = Duration.ofSeconds(3);
    protected Duration uploadRetryTimeout = Duration.ofSeconds(1);

    protected boolean stopped = true;
    protected long actionsBytes;
    protected Instant lastFlushTime = Instant.MAX;
    protected ArrayList<Action> actionList = new ArrayList<>();
    protected Thread uploadThread;

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

    public void setUploadRetryTimeout(Duration uploadRetryTimeout) {
        this.uploadRetryTimeout = uploadRetryTimeout;
    }

    /**
     * Starts uploading
     */
    public synchronized void start() {
        uploadThread = new Thread(this::upload, "appmetr-upload-" + token);
        uploadThread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught upload exception", e));
        uploadThread.start();

        stopped = false;
    }

    /**
     * Does flush and stop uploading.
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
        } catch (IOException e) {
            log.error("Exception while stopping", e);
        }
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if an Action has been tracked successfully or {@code false} if no space is currently available.
     */
    public synchronized boolean track(Action newAction) throws InterruptedException, IOException {
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
    public synchronized boolean flush() throws InterruptedException, IOException {
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

    public synchronized void flushIfNeeded() throws InterruptedException, IOException {
        if (needFlush()) {
            flush();
        }
    }

    protected boolean needFlush() {
        return actionsBytes >= maxBatchBytes
                || (maxBatchActions > 0 && actionList.size() > maxBatchActions)
                || clock.instant().minus(flushPeriod).isAfter(lastFlushTime);
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
                binaryBatch = batchStorage.peek();
            } catch (InterruptedException e) {
                break;
            } catch (IOException e) {
                log.error("Error while reading batch", e);
                try {
                    Thread.sleep(readRetryTimeout.toMillis());
                    continue;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            allBatchCounter++;

            log.trace("Batch {} read time: {} ms", binaryBatch.getBatchId(), Duration.between(batchReadStart, clock.instant()));

            while (true) {
                final Instant batchUploadStart = clock.instant();
                boolean result;
                try {
                    result = batchSender.send(url, token, binaryBatch.getBytes());
                } catch (Exception e) {
                    log.warn("Exception while sending the batch {}", binaryBatch.getBatchId(), e);
                    result = false;
                }

                log.debug("Batch {} {} finished. Took {} ms", binaryBatch.getBatchId(), result ? "" : "NOT", Duration.between(batchUploadStart, clock.instant()));

                if (result) {
                    log.trace("Batch {} successfully uploaded", binaryBatch.getBatchId());
                    tryRemove(binaryBatch.getBatchId());
                    uploadedBatchCounter++;
                    sendBatchesBytes += binaryBatch.getBytes().length;
                    break;
                }

                log.error("Error while uploading batch {}", binaryBatch.getBatchId());

                try {
                    Thread.sleep(uploadRetryTimeout.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (!retryBatchUpload) {
                    tryRemove(binaryBatch.getBatchId());
                }

                log.info("Retrying the batch {}", binaryBatch.getBatchId());
            }

            if (Thread.interrupted()) {
                break;
            }
        }

        log.info("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
        Thread.currentThread().interrupt();
    }

    protected void tryRemove(long batchId) {
        try {
            batchStorage.remove();
        } catch (IOException e) {
            log.error("Error while removing uploaded batch {}", batchId);
        }
    }
}
