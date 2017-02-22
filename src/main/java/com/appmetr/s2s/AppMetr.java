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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class AppMetr {
    private static final Logger log = LoggerFactory.getLogger(AppMetr.class);

    private static final int MILLIS_PER_MINUTE = 1000 * 60;
    private static final long FLUSH_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final long UPLOAD_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final int MAX_EVENTS_SIZE = FileBatchPersister.REBATCH_THRESHOLD_FILE_SIZE;
    private static final int MAX_EVENTS_COUNT = FileBatchPersister.REBATCH_THRESHOLD_ITEM_COUNT;

    public static final String SERVER_ID = UUID.randomUUID().toString();

    private final String token;
    private final String url;
    private final BatchPersister batchPersister;
    private final ScheduledExecutorService flushExecutor;
    private final ScheduledExecutorService uploadExecutor;
    private final boolean needFlushShutdown;
    private final boolean needUploadShutdown;
    private volatile boolean stopped = false;

    private int eventsSize;
    private ArrayList<Action> actionList = new ArrayList<>();
    private final Lock flushLock = new ReentrantLock();
    private final Lock uploadLock = new ReentrantLock();
    private final Lock listLock = new ReentrantLock();

    private volatile Future<?> flushFuture;
    private volatile Future<?> uploadFuture;
    private final AtomicBoolean flushSubmitted = new AtomicBoolean();

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
        this.flushExecutor = flushExecutor;
        this.needFlushShutdown = needFlushShutdown;
        if (uploadExecutor != null) {
            this.uploadExecutor = uploadExecutor;
            this.needUploadShutdown = needUploadShutdown;
        } else {
            this.uploadExecutor = flushExecutor;
            this.needUploadShutdown = false;
        }

        scheduleFlush();
        scheduleUpload();
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
            submitFlush();
        }
    }

    protected void flush() {
        log.debug("Flushing started for {} actions", actionList.size());

        final ArrayList<Action> actionsToPersist;
        listLock.lock();
        try {
            if (actionList.isEmpty()) {
                log.info("Nothing to flush");
                return;
            }

            actionsToPersist = actionList;
            actionList = new ArrayList<>();
            eventsSize = 0;
        } finally {
            listLock.unlock();
        }

        batchPersister.persist(actionsToPersist);
        log.info("Flushing completed");

        submitUpload();
    }

    protected void scheduleFlush() {
        flushFuture = flushExecutor.schedule(runIfNotLocked(flushLock, this::flush , flushExecutor, getFlushPeriod(),
                future -> flushFuture = future), getFlushPeriod(), TimeUnit.MILLISECONDS);
    }

    protected void scheduleUpload() {
        uploadFuture = uploadExecutor.schedule(runIfNotLocked(uploadLock, this::upload, uploadExecutor, getUploadPeriod(),
                future -> uploadFuture = future), getFlushPeriod()/2 + getUploadPeriod(), TimeUnit.MILLISECONDS);
    }

    protected Future<?> submitFlush() {
        do {
            if (flushSubmitted.get()) {
                return flushFuture;
            }
        } while (!flushSubmitted.compareAndSet(false, true));

        return submitMethod(flushLock, flushFuture, this::flush, flushExecutor, getFlushPeriod(),
                future -> {flushFuture = future; flushSubmitted.set(false);});
    }

    protected Future<?> submitUpload() {
        return submitMethod(uploadLock, uploadFuture, this::upload, uploadExecutor, getUploadPeriod(), future -> uploadFuture = future);
    }

    protected Future<?> submitMethod(Lock lock, Future<?> future, Runnable runnable,
                                     ScheduledExecutorService executor, long delay, Consumer<Future<?>> futureConsumer) {
        if (future != null) {
            future.cancel(false);
        }

        final Future<?> newFuture = executor.submit(runIfNotLocked(lock, runnable, executor, delay, futureConsumer));
        futureConsumer.accept(newFuture);
        return newFuture;
    }

    protected boolean isNeedToFlush() {
        return eventsSize >= MAX_EVENTS_SIZE || actionList.size() >= MAX_EVENTS_COUNT;
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
            final long batchReadStart = System.currentTimeMillis();
            final byte[] batchBytes = SerializationUtils.serializeJsonGzip(batch, false);
            final long batchReadEnd = System.currentTimeMillis();

            log.trace("Batch {} read time: {} ms", batch.getBatchId(), batchReadEnd - batchReadStart);

            final long batchUploadStart = System.currentTimeMillis();
            try {
                result = HttpRequestService.sendRequest(url, token, batchBytes);
            } catch (IOException e) {
                log.error("IOException while sending request", e);
                break;
            }
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

            if (!result) {
                break;
            }
        }

        log.info("{} from {} batches uploaded. ({} bytes)", uploadedBatchCounter, allBatchCounter, sendBatchesBytes);
    }

    protected Runnable runIfNotLocked(final Lock lock, final Runnable runnable,
                                      ScheduledExecutorService executor, long delay, Consumer<Future<?>> futureConsumer) {
        return new Runnable() {
            @Override public void run() {
                if (lock.tryLock()) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        log.error("Exception during execution", e);
                    } finally {
                        futureConsumer.accept(executor.schedule(this, delay, TimeUnit.MILLISECONDS));
                        lock.unlock();
                    }
                }
            }
        };
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
            submitFlush().get();
            flushFuture.cancel(false);

            uploadFuture.get();
            uploadFuture.cancel(false);

            if (needFlushShutdown) {
                flushExecutor.shutdownNow();
            }
            if (needUploadShutdown) {
                uploadExecutor.shutdownNow();
            }

        } catch (InterruptedException e) {
            log.error("Stop was interrupted", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Exception while execution", e);
        }
    }
}
