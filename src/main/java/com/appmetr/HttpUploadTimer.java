package com.appmetr;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class HttpUploadTimer implements Runnable {

    protected static final Logger logger = Logger.getLogger("HttpUploadTimer");

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private volatile Thread pollingThread = null;

//    private static final long UPLOAD_PERIOD = DateTimeConstants.MILLIS_PER_MINUTE * 5;
    private static final long UPLOAD_PERIOD = DateTimeConstants.MILLIS_PER_SECOND * 30; //TODO: change that

    private final AppMetr appMetr;

    public HttpUploadTimer(AppMetr appMetr){
        //TODO: change AppMetr to Singleton and delete this constructor
        this.appMetr = appMetr;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info("HttpUploadTimer started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(UPLOAD_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("HttpUploadTimer - run uploader");
                appMetr.upload();
            } catch (InterruptedException ie) {
                logger.info("Interrupted while polling the queue. Stop polling");

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.info("Error while uploading batch: " + e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("HttpUploadTimer stoped!");
    }

    public void trigger() {
        lock.lock();

        try {
            trigger.signal();
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        logger.info("HttpUploadTimer stop triggered!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
