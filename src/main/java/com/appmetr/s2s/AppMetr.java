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
import java.util.concurrent.atomic.AtomicInteger;

public class AppMetr {
    protected static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    public static final String SERVER_ID = UUID.randomUUID().toString();

    private final String token;
    private final String url;
    private volatile boolean stopped = false;

    private final AtomicInteger eventsSize = new AtomicInteger(0);
    private final ArrayList<Action> actionList = new ArrayList<>();

    protected AppMetrTimer eventFlushTimer;
    protected AppMetrTimer httpUploadTimer;

    private static final int MILLIS_PER_MINUTE = 1000 * 60;
    private static final long FLUSH_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final long UPLOAD_PERIOD = MILLIS_PER_MINUTE / 2;

    private static final int MAX_EVENTS_SIZE = FileBatchPersister.REBATCH_THRESHOLD_FILE_SIZE;
    private static final int MAX_EVENTS_COUNT = FileBatchPersister.REBATCH_THRESHOLD_ITEM_COUNT;

    private BatchPersister batchPersister;

    public AppMetr(String token, String url, BatchPersister persister) {
        this.url = url;
        this.token = token;
        this.batchPersister = persister == null ? new MemoryBatchPersister() : persister;

        eventFlushTimer = new AppMetrTimer(FLUSH_PERIOD, new Runnable() {
            @Override public void run() {
                flush();
            }
        }, "FlushTask");
        eventFlushTimer.start();

        httpUploadTimer = new AppMetrTimer(UPLOAD_PERIOD, new Runnable() {
            @Override public void run() {
                upload();
            }
        }, "UploadTask");
        httpUploadTimer.start();
    }

    public AppMetr(String token, String url) {
        this(token, url, null);
    }

    public void track(Action newAction) {
        if (stopped) {
            throw new RuntimeException("Trying to track after stop!");
        }

        try {
            synchronized (actionList) {
                eventsSize.addAndGet(newAction.calcApproximateSize());
                actionList.add(newAction);
            }

            if (isNeedToFlush()) {
                eventFlushTimer.trigger();
            }
        } catch (Exception e) {
            log.error("Track failed", e);
        }
    }

    protected void flush() {
        log.debug("Flushing started for {} actions", actionList.size());

        ArrayList<Action> copyAction;
        synchronized (actionList) {
            copyAction = new ArrayList<>(actionList);
            actionList.clear();
            eventsSize.set(0);
        }

        if (copyAction.size() > 0) {
            batchPersister.persist(copyAction);
            httpUploadTimer.trigger();
        } else {
            log.info("Nothing to flush");
        }

        log.info("Flushing completed");
    }

    protected boolean isNeedToFlush() {
        return eventsSize.get() >= MAX_EVENTS_SIZE || actionList.size() >= MAX_EVENTS_COUNT;
    }

    protected void upload() {
        log.info("Upload starting");

        Batch batch;
        int uploadedBatchCounter = 0;
        int allBatchCounter = 0;
        long sendBatchesBytes = 0;
        while ((batch = batchPersister.getNext()) != null) {
            allBatchCounter++;

            boolean result;
            try {
                final long batchReadStart = System.currentTimeMillis();
                final byte[] batchBytes = SerializationUtils.serializeJsonGzip(batch, false);
                final long batchReadEnd = System.currentTimeMillis();

                log.trace("Batch {} read time: {} ms", batch.getBatchId(), batchReadEnd - batchReadStart);

                final long batchUploadStart = System.currentTimeMillis();
                result = HttpRequestService.sendRequest(url, token, batchBytes);
                final long batchUploadEnd = System.currentTimeMillis();


                if (result) {
                    log.trace("Batch {} successfully uploaded", batch.getBatchId());
                    batchPersister.remove();
                    uploadedBatchCounter++;
                    sendBatchesBytes += batchBytes.length;
                } else {
                    log.error("Error while upload batch {}. Took {} ms", batch.getBatchId(), batchUploadEnd - batchUploadStart);
                }
                log.info("Batch {} {} finished. Took {} ms", batch.getBatchId(), result ? "" : "NOT", batchUploadEnd - batchUploadStart);

                if (!result) break;

            } catch (IOException e) {
                log.error("IOException while sending request", e);
                break;
            }
        }

        log.info("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
    }

    public void stop() {
        stopped = true;

        try {
            Thread.sleep(10);

            eventFlushTimer.triggerAndStop();
            eventFlushTimer.join();

            httpUploadTimer.triggerAndStop();
            httpUploadTimer.join();

        } catch (InterruptedException e) {
            log.error("Stop was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
