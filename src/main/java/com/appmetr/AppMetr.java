package com.appmetr;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class AppMetr {
    protected static final Logger logger = Logger.getLogger("AppMetr");//LoggerFactory.getLogger("AppMetr");

    protected final Lock fileWritterLock = new ReentrantLock();
    protected final Lock flushLock = new ReentrantLock();

    private final String token;
    private AtomicInteger eventsSize = new AtomicInteger(0);
    private final ArrayList<Event> eventList = new ArrayList<Event>();

    protected File file;
    protected FileWriter fileWriter;
    protected EventFlusher eventFlusher;
    protected BatchPusher batchPusher;

    private static final int MAX_EVENTS_SIZE = 500 * 1024 * 1024;
    private static final int MAX_FILE_SIZE =  1024 * 100; //100 kb

    public AppMetr(String token){
        this.token = token;
        eventFlusher = new EventFlusher(this);
        eventFlusher.run();

        batchPusher = new BatchPusher();
        batchPusher.run();
    }

    public void track(String eventName, Map<String, String> properties){
        Event event = new Event(eventName, properties);
        track(event);
    }

    public void track(Event newEvent){
        try {
            eventsSize.addAndGet(newEvent.calcApproximateSize());
            if(isNeedToFlush()){
                eventFlusher.trigger();
            }

            synchronized (eventList) {
                eventList.add(newEvent);
            }
        } catch (Exception error) {
            logger.info("track failed " + error);
        }
    }

    protected void flush(){
        flushLock.lock();
        logger.info("flushing started");

        flushDataImpl();

        logger.info("flushing completed");
        flushLock.unlock();
    }

    protected void flushDataImpl() {
        ArrayList<Event> copyEvent;
        synchronized (eventList) {
            copyEvent = new ArrayList<Event>(eventList);
            eventList.clear();
        }

        if (copyEvent.size() > 0) {
            fileWritterLock.lock();
            try {
                int batchId = BatchIdRandomGenerator.getBatchId();
                String encodedString = Integer.toString(batchId);

                int copyEventSize = 0;
                for(Event event : copyEvent){
                    copyEventSize += event.calcApproximateSize();
                }

                if (fileWriter != null && encodedString.length() + copyEventSize + file.length() > MAX_FILE_SIZE){
                    closeCurrentFileWriter();
                }

                if (fileWriter == null) {
                    file = new File(FileNamer.getNextFileName());
                    fileWriter = new FileWriter(file, true);
                }

                fileWriter.append(encodedString);
            } catch (Exception error) {
                logger.info("Failed to save the data to disc. " + error);
            }

            fileWritterLock.unlock();
        }
    }

    private void closeCurrentFileWriter(){
        fileWritterLock.lock();

        try {
            if (fileWriter != null) {
                fileWriter.close();
            }
        } catch (final IOException e) {
            logger.info("IOException: " + e);
        }

        fileWriter = null;
        fileWritterLock.unlock();
    }

    protected boolean isNeedToFlush() {
        return eventsSize.get() >= MAX_EVENTS_SIZE;
    }
}
