package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.persister.BatchPersister;
import com.appmetr.s2s.persister.FileBatchPersister;
import com.appmetr.s2s.persister.MemoryBatchPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetr {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    private static final int MILLIS_PER_MINUTE = 1000 * 60;
    private static final long FLUSH_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final long UPLOAD_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final long MAX_EVENTS_SIZE = FileBatchPersister.REBATCH_THRESHOLD_FILE_SIZE;
    private static final int MAX_EVENTS_COUNT = FileBatchPersister.REBATCH_THRESHOLD_ITEM_COUNT;

    private final String token;
    private final String url;
    private final BatchPersister batchPersister;
    private final ScheduledExecutorService flushExecutor;
    private final ScheduledExecutorService uploadExecutor;
    private final boolean needFlushShutdown;
    private final boolean needUploadShutdown;
    private volatile boolean stopped = false;

    private long eventsSize;
    private ArrayList<Action> actionList = new ArrayList<>();
    private final Lock listLock = new ReentrantLock();

    private final ScheduledAndForced flushSchedule;
    private final ScheduledAndForced uploadSchedule;

    public AppMetr(String token, String url, BatchPersister persister) {
        this(token, url, persister,
                Executors.newSingleThreadScheduledExecutor(), true,
                Executors.newSingleThreadScheduledExecutor(), true);
    }

    public AppMetr(String token, String url, BatchPersister persister,
                   ScheduledExecutorService flushExecutor, boolean needFlushShutdown,
                   ScheduledExecutorService uploadExecutor, boolean needUploadShutdown) {
        this.url = url;
        this.token = token;
        this.batchPersister = persister;
        persister.setServerId(UUID.randomUUID().toString());
        this.flushExecutor = flushExecutor;
        this.needFlushShutdown = needFlushShutdown;
        if (uploadExecutor != null) {
            this.uploadExecutor = uploadExecutor;
            this.needUploadShutdown = needUploadShutdown;
        } else {
            this.uploadExecutor = flushExecutor;
            this.needUploadShutdown = false;
        }

        flushSchedule = new ScheduledAndForced(this.flushExecutor, this::flush, getFlushPeriod());
        uploadSchedule = new ScheduledAndForced(this.uploadExecutor, this::upload, getFlushPeriod() / 2, getUploadPeriod());
    }

    public AppMetr(String token, String url) {
        this(token, url, new MemoryBatchPersister());
    }

    public void track(Action newAction) {
        if (stopped) {
            throw new RuntimeException("Trying to track after stop!");
        }

        final boolean needFlush;

        listLock.lock();
        try {
            eventsSize += newAction.calcApproximateSize();
            actionList.add(newAction);
            needFlush = isNeedToFlush();
        }
        finally {
            listLock.unlock();
        }

        if (needFlush) {
            flushSchedule.force();
        }
    }

    protected void flush() {
        log.debug("Flushing started for {} actions", actionList.size());

        final ArrayList<Action> actionsToPersist;
        listLock.lock();
        try {
            if (actionList.isEmpty()) {
                log.debug("Nothing to flush");
                return;
            }

            actionsToPersist = actionList;
            actionList = new ArrayList<>();
            eventsSize = 0;
        } finally {
            listLock.unlock();
        }

        batchPersister.persist(actionsToPersist);
        log.debug("Flushing completed");

        uploadSchedule.force();
    }

    protected boolean isNeedToFlush() {
        return eventsSize >= MAX_EVENTS_SIZE || actionList.size() >= MAX_EVENTS_COUNT;
    }

    protected void upload() {
        log.debug("Upload starting");

        Batch batch;
        int uploadedBatchCounter = 0;
        int allBatchCounter = 0;
        long sendBatchesBytes = 0;
        while ((batch = batchPersister.getNext()) != null) {
            allBatchCounter++;

            boolean result;
            final long batchReadStart = System.currentTimeMillis();
            final byte[] batchBytes = SerializationUtils.serializeJsonGzip(batch, false);
            final long batchReadEnd = System.currentTimeMillis();

            log.trace("Batch {} read time: {} ms", batch.getBatchId(), batchReadEnd - batchReadStart);

            final long batchUploadStart = System.currentTimeMillis();
            try {
                result = HttpRequestService.sendRequest(url, token, batchBytes);
            } catch (IOException e) {
                log.error("IOException while sending request", e);
                result = false;
            }
            final long batchUploadEnd = System.currentTimeMillis();

            log.debug("Batch {} {} finished. Took {} ms", batch.getBatchId(), result ? "" : "NOT", batchUploadEnd - batchUploadStart);

            if (result) {
                log.trace("Batch {} successfully uploaded", batch.getBatchId());
                batchPersister.remove();
                uploadedBatchCounter++;
                sendBatchesBytes += batchBytes.length;
            } else {
                log.error("Error while upload batch {}. Took {} ms", batch.getBatchId(), batchUploadEnd - batchUploadStart);
                break;
            }
        }

        log.debug("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
    }

    protected long getFlushPeriod() {
        return FLUSH_PERIOD;
    }

    protected long getUploadPeriod() {
        return UPLOAD_PERIOD;
    }

    /**
     * Does flush and then upload all pending actions.
     * Stops inner threads.
     * May requires some time for competition even no pending actions exist.
     */
    public void stop() {
        stopped = true;

        try {
            flushSchedule.stop();
            flush();

            uploadSchedule.stop();

            if (needFlushShutdown) {
                flushExecutor.shutdown();
            }
            if (needUploadShutdown) {
                uploadExecutor.shutdown();
            }

        } catch (InterruptedException e) {
            log.error("AppMetr stopping was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
