package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.persister.BatchFactoryServerId;
import com.appmetr.s2s.persister.BatchStorage;
import com.appmetr.s2s.persister.GzippedJsonBatchFactory;
import com.appmetr.s2s.persister.HeapStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

public class AppMetr {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    public static final int DEFAULT_MAX_BATCH_ACTIONS = 1000;
    public static final long DEFAULT_MAX_BATCH_BYTES = 1024 * 1024;
    public static final Duration DEFAULT_FLUSH_PERIOD = Duration.ofMinutes(1);

    private String token;
    private String url;
    private boolean retryBatchUpload;
    protected String serverId = UUID.randomUUID().toString();
    private Clock clock = Clock.systemUTC();
    private BatchStorage batchStorage = new HeapStorage();
    private HttpRequestService httpRequestService = new HttpRequestService();
    private BatchFactoryServerId batchFactory = new GzippedJsonBatchFactory();

    private int maxBatchActions = DEFAULT_MAX_BATCH_ACTIONS;
    private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
    private Duration flushPeriod = DEFAULT_FLUSH_PERIOD;

    private boolean stopped = true;
    private long actionsBytes;
    private Instant lastUploadTime = Instant.MAX;
    private ArrayList<Action> actionList = new ArrayList<>();
    private Thread uploadThread;

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
        lastUploadTime = clock.instant();

        return true;
    }

    /**
     * Can blocks until a storage space become available regardless BatchStorage implementation
     * @return {@code true} if the actions was added to this storage, else
     *         {@code false}
     */
    protected synchronized boolean flush() throws InterruptedException {
        log.trace("Flushing started for {} actions", actionList.size());

        if (actionList.isEmpty()) {
            log.debug("Nothing to flush");
            return true;
        }

        final boolean stored = batchStorage.store(actionList, (actions, batchId) -> batchFactory.createBatch(actions, batchId, serverId));
        if (stored) {
            log.debug("Flushing completed for {} actions", actionList.size());
            actionList.clear();
            actionsBytes = 0;
        } else {
            log.warn("Flushing failed for {} actions", actionList.size());
        }

        return stored;
    }

    protected boolean needFlush() {
        return actionsBytes >= maxBatchBytes || actionList.size() >= maxBatchActions;
    }

    protected void upload() {
        log.debug("Upload starting");

        Batch batch;
        int uploadedBatchCounter = 0;
        int allBatchCounter = 0;
        long sendBatchesBytes = 0;
        while ((batch = batchStorage.getNext()) != null) {
            allBatchCounter++;

            boolean result;
            final long batchReadStart = System.currentTimeMillis();
            final byte[] batchBytes = SerializationUtils.serializeJsonGzip(batch, false);
            final long batchReadEnd = System.currentTimeMillis();

            log.trace("Batch {} read time: {} ms", batch.getBatchId(), batchReadEnd - batchReadStart);

            final long batchUploadStart = System.currentTimeMillis();
            try {
                result = httpRequestService.sendRequest(url, token, batchBytes);
            } catch (IOException e) {
                log.error("IOException while sending request", e);
                result = false;
            }
            final long batchUploadEnd = System.currentTimeMillis();

            log.debug("Batch {} {} finished. Took {} ms", batch.getBatchId(), result ? "" : "NOT", batchUploadEnd - batchUploadStart);

            if (result) {
                log.trace("Batch {} successfully uploaded", batch.getBatchId());
                batchStorage.remove();
                uploadedBatchCounter++;
                sendBatchesBytes += batchBytes.length;
            } else {
                log.error("Error while upload batch {}. Took {} ms", batch.getBatchId(), batchUploadEnd - batchUploadStart);
                if (retryBatchUpload) {
                    break;
                }
                batchStorage.remove();
            }
        }

        log.debug("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
    }
}
