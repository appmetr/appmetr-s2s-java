package com.appmetr;

import java.util.concurrent.atomic.AtomicInteger;

public class FileNamer {
    public static final String batchFileName = "batchFile";
    private static AtomicInteger currentFileIndex = new AtomicInteger(0);

    public static String getNextFileName(){
        int fileIndex = currentFileIndex.incrementAndGet();
        return batchFileName + fileIndex;
    }
}
