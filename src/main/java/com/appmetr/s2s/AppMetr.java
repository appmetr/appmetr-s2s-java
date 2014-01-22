package com.appmetr.s2s;

import com.appmetr.s2s.persister.BatchPersister;
import com.appmetr.s2s.persister.MemoryBatchPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AppMetr {
    protected static final Logger logger = LoggerFactory.getLogger(AppMetr.class);

    protected final Lock flushLock = new ReentrantLock();
    protected final Lock uploadLock = new ReentrantLock();

    private final String token;
    private final String url;

    private AtomicInteger eventsSize = new AtomicInteger(0);
    private final ArrayList<Event> eventList = new ArrayList<Event>();

    protected AppMetrTimer eventFlushTimer;
    protected AppMetrTimer httpUploadTimer;

    private static final int MILLIS_PER_MINUTE = 1000 * 60;
    private static final long FLUSH_PERIOD = 1000 * 30;//TODO: change
    private static final long UPLOAD_PERIOD = 1000 * 10;//TODO: change

    private static final int MAX_EVENTS_SIZE = 1024 * 500;

    private BatchPersister batchPersister = new MemoryBatchPersister();

    public AppMetr(String token, String url) {
        this.url = url;
        this.token = token;

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

    public void track(String eventName, Map<String, String> properties) {
        Event event = new Event(eventName, properties);
        track(event);
    }

    public void track(Event newEvent) {
        try {
            synchronized (eventList) {
                eventsSize.addAndGet(newEvent.calcApproximateSize());
                eventList.add(newEvent);
            }

            if (isNeedToFlush()) {
                eventFlushTimer.trigger();
            }
        } catch (Exception error) {
            logger.error("Track failed", error);
        }
    }

    protected void flush() {
        flushLock.lock();
        logger.info("Flushing started");

        ArrayList<Event> copyEvent;
        synchronized (eventList) {
            copyEvent = new ArrayList<Event>(eventList);
            eventList.clear();
            eventsSize.set(0);
        }

        if (copyEvent != null && copyEvent.size() > 0) {
            batchPersister.persist(copyEvent);
            httpUploadTimer.trigger();
        } else {
            logger.info("Nothing to flush");
        }

        logger.info("Flushing completed");
        flushLock.unlock();
    }

    protected boolean isNeedToFlush() {
        return eventsSize.get() >= MAX_EVENTS_SIZE;
    }

    protected void upload() {
        uploadLock.lock();
        logger.info("Upload starting");

        Batch batch = batchPersister.getNext();
        boolean result = false;
        if (batch != null) {
            try {
                result = HttpRequestService.sendRequest(url, token, SerializationUtils.serializeGzip(batch));
                if (result) batchPersister.remove();
            } catch (IOException e) {
                logger.error("IOException while sending request", e);
            }
        } else {
            logger.info("Nothing to upload");
        }

        logger.info(String.format("Upload completed, status: %s", result ? "success" : "fails"));
        uploadLock.unlock();
    }

    public void stop() {
        eventFlushTimer.stop();
        httpUploadTimer.stop();
    }
}
