package com.appmetr;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class BatchPusher implements Runnable{
    private Logger logger = Logger.getLogger("BatchPusher");

    private volatile Thread pollingThread = null;
    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private static final Charset encoding = Charset.defaultCharset();
    private static final long SEND_PERIOD = DateTimeConstants.MILLIS_PER_MINUTE * 5;

    @Override public void run() {
        pollingThread = Thread.currentThread();
        logger.info("BatchPusher started!");

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                trigger.await(SEND_PERIOD, TimeUnit.MILLISECONDS);

                logger.info("BatchPusher - run push");
                String stringToPush = getStringToPush();
                if(stringToPush != null && !stringToPush.isEmpty()) {
                    HtmlPusherService.sendGzippedPost("http://localhost",stringToPush);
                }
            } catch (InterruptedException ie) {
                logger.info("Interrupted while pushing the file. Stop pushing");

                pollingThread.interrupt();
            } catch (Exception e) {
                logger.info("Error while pushing: " + e);
            } finally {
                lock.unlock();
            }
        }

        logger.info("BatchPusher stoped!");
    }

    private String getStringToPush(){
        File[] listOfFiles = new File("").listFiles();

        ArrayList<Integer> fileIndexes = new ArrayList<Integer>(listOfFiles.length);
        for(File file : listOfFiles){
            String fileName = file.getName();
            if(fileName.startsWith(FileNamer.batchFileName)){
                int fileIndex = Integer.parseInt(fileName.substring(FileNamer.batchFileName.length()));
                fileIndexes.add(fileIndex);
            }
        }

        if(fileIndexes.size() > 0){
            int min = Integer.MAX_VALUE;
            for(int index : fileIndexes){
                if(index < min) min = index;
            }

            byte[] encoded = null;
            Path file = null;
            try{
                file = Paths.get(FileNamer.batchFileName + min);
                encoded = Files.readAllBytes(file);
            } catch (IOException e){
                logger.info("IOException: " + e);
            }

            if(encoded != null){
                if (file != null){
                    try {
                        Files.delete(file);
                    } catch(IOException e){
                        logger.info("Cant delete file: " + e);
                    }
                }
                return encoding.decode(ByteBuffer.wrap(encoded)).toString();
            }
        }

        return null;
    }

    public void stop() {
        logger.info("EventFlusher stop triggered!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
