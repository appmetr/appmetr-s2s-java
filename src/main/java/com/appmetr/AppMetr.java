package com.appmetr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class AppMetr {
    protected static final Logger logger = Logger.getLogger("AppMetr");//LoggerFactory.getLogger("AppMetr");

    protected final Lock flushLock = new ReentrantLock();
    protected final Lock uploadLock = new ReentrantLock();

    private final String token;
    private final String url;

    private AtomicInteger eventsSize = new AtomicInteger(0);
    private final ArrayList<Event> eventList = new ArrayList<Event>();

    protected EventFlushTimer eventFlushTimer;
    protected HttpUploadTimer httpUploadTimer;

    private static final int MAX_EVENTS_SIZE = 1024 * 500;//TODO: change that

    private BatchPersister batchPersister = new MemoryBatchPersister();

    public AppMetr(String token, String url){
        this.url = url;
        this.token = token;

        eventFlushTimer = new EventFlushTimer(this);
        Thread eventFlushTimerThread = new Thread(eventFlushTimer);
        eventFlushTimerThread.start();

        httpUploadTimer = new HttpUploadTimer(this);
        Thread httpUploadTimerThread = new Thread(httpUploadTimer);
        httpUploadTimerThread.start();
    }

    public void track(String eventName, Map<String, String> properties){
        Event event = new Event(eventName, properties);
        track(event);
    }

    public void track(Event newEvent){
        try {
            eventsSize.addAndGet(newEvent.calcApproximateSize());

            synchronized (eventList) {
                eventList.add(newEvent);
            }

            if(isNeedToFlush()){
                eventFlushTimer.trigger();
            }
        } catch (Exception error) {
            logger.info("track failed " + error);
        }
    }

    protected void flush(){
        flushLock.lock();
        logger.info("flushing started");

        ArrayList<Event> copyEvent;
        synchronized (eventList) {
            copyEvent = new ArrayList<Event>(eventList);
            eventList.clear();
        }

        if(copyEvent.size() > 0){
            batchPersister.persist(copyEvent);
            httpUploadTimer.trigger();
        }

        logger.info("flushing completed");
        flushLock.unlock();
    }

    protected boolean isNeedToFlush() {
        return eventsSize.get() >= MAX_EVENTS_SIZE;
    }

    protected void upload(){
        uploadLock.lock();
        logger.info("upload starting");

        Batch batch = batchPersister.getNextBatch();
        boolean result = false;
        if(batch != null){
            try{
                result = HttpRequestService.sendRequest(url, token, SerializationUtils.serializeGzip(batch));
                if(result) batchPersister.deleteLastBatch(batch.getBatchId());
            } catch(IOException e){
                logger.info("IOException while sending request " + e);
            }
        }else{
            logger.info("nothing to upload");
        }

        logger.info("upload complete, success = " + result);
        uploadLock.unlock();
    }

    protected void stop(){
        eventFlushTimer.stop();
        httpUploadTimer.stop();
    }
}
