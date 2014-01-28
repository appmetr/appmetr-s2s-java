package com.appmetr.s2s;

import com.appmetr.s2s.persister.BatchPersister;
import com.appmetr.s2s.persister.MemoryBatchPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetr {
    protected static final Logger logger = LoggerFactory.getLogger(AppMetr.class);

    protected final ReentrantLock flushLock = new ReentrantLock();
    protected final ReentrantLock uploadLock = new ReentrantLock();

    private final String token;
    private final String url;
    private boolean stopped = false;

    private AtomicInteger eventsSize = new AtomicInteger(0);
    private final ArrayList<Event> eventList = new ArrayList<Event>();

    protected AppMetrTimer eventFlushTimer;
    protected AppMetrTimer httpUploadTimer;

    private static final int MILLIS_PER_MINUTE = 1000 * 60;
    private static final long FLUSH_PERIOD = MILLIS_PER_MINUTE / 2;
    private static final long UPLOAD_PERIOD = MILLIS_PER_MINUTE / 2;

    private static final int MAX_EVENTS_SIZE = 1024 * 500 * 20;

    private BatchPersister batchPersister;

    public AppMetr(String token, String url, BatchPersister persister) {
        this.url = url;
        this.token = token;
        this.batchPersister = persister == null ? new MemoryBatchPersister() : persister;

        eventFlushTimer = new AppMetrTimer(FLUSH_PERIOD, new Runnable() {
            @Override public void run() {
                flush();
            }
        });
        new Thread(eventFlushTimer).start();

        httpUploadTimer = new AppMetrTimer(UPLOAD_PERIOD, new Runnable() {
            @Override public void run() {
                upload();
            }
        });
        new Thread(httpUploadTimer).start();
    }

    public AppMetr(String token, String url) {
        this(token, url, null);
    }

    public void track(String eventName, Map<String, Object> properties) {
        Event event = new Event(eventName, new Date().getTime(), properties);
        track(event);
    }

    public void track(Event newEvent) {
        if (stopped) {
            throw new RuntimeException("Trying to track after stop!");
        }

        try {
            boolean flushNeeded;
            synchronized (eventList) {
                eventsSize.addAndGet(newEvent.calcApproximateSize());
                eventList.add(newEvent);

                flushNeeded = isNeedToFlush();
            }
            if (flushNeeded) {
                eventsSize.set(0);
                eventFlushTimer.trigger();
            }
        } catch (Exception error) {
            logger.error("Track failed", error);
        }
    }

    protected void flush() {
        flushLock.lock();
        try {
            logger.info("Flushing started");

            ArrayList<Event> copyEvent;
            synchronized (eventList) {
                copyEvent = new ArrayList<Event>(eventList);
                eventList.clear();
            }

            if (copyEvent != null && copyEvent.size() > 0) {
                batchPersister.persist(copyEvent);
                httpUploadTimer.trigger();
            } else {
                logger.info("Nothing to flush");
            }

            logger.info("Flushing completed");
        } finally {
            flushLock.unlock();
        }
    }

    protected boolean isNeedToFlush() {
        return eventsSize.get() >= MAX_EVENTS_SIZE;
    }

    protected void upload() {
        uploadLock.lock();
        try {
            logger.info("Upload starting");

            Batch batch = batchPersister.getNext();
            boolean result = false;
            if (batch != null) {
                try {
                    result = HttpRequestService.sendRequest(url, token, SerializationUtils.serializeJsonGzip(batch));
                    if (result) batchPersister.remove();
                } catch (IOException e) {
                    logger.error("IOException while sending request", e);
                }
            } else {
                logger.info("Nothing to upload");
            }

            logger.info(String.format("Upload completed, status: %s", result ? "success" : "fails"));
        } finally {
            uploadLock.lock();
        }
    }

    public void stop() {
        stopped = true;

        uploadLock.lock();
        try {
            httpUploadTimer.stop();
        } finally {
            uploadLock.lock();
        }

        flushLock.lock();
        try {
            eventFlushTimer.stop();
        } finally {
            flushLock.unlock();
        }

        flush();
    }
}
