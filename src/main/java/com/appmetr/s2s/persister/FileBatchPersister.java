package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.Event;
import com.appmetr.s2s.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileBatchPersister implements BatchPersister {
    private final static Logger logger = LoggerFactory.getLogger(BatchPersister.class);

    private final Object writeLock = new Object();
    private final Object readLock = new Object();

    private File path;
    private int firstFileId;
    private int lastBatchId;

    private int BATCHSIZE_BYTES_COUNT = 100;

    private final File batchIdFileSaver;
    private final String BATCH_FILE_NAME = "batchFile#";
    private final String DIGITAL_FORMAT = "%011d";

    public FileBatchPersister(String filePath) {
        path = new File(filePath);

        if (!path.exists()) {
            path.mkdirs();
        }

        if (!path.isDirectory()) {
            path = path.getParentFile();
        }

        batchIdFileSaver = new File(path.getAbsolutePath() + "/lastBatchId");

        lastBatchId = getLastBatchId();

        fillFirstFileId();
    }

    private int getLastBatchId() {
        if (batchIdFileSaver.exists() && batchIdFileSaver.length() > 0) {
            int batchId = 0;

            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(batchIdFileSaver), Charset.forName("UTF-8")));
                batchId = Integer.parseInt(br.readLine());
            } catch (IOException e) {
                logger.warn("Exception while reading from batchIdFileSaver: ", e);
            } finally {
                try {
                    br.close();
                } catch (IOException e) {
                    logger.warn("Exception while closing batchIdFileSaver file: ", e);
                }

                return batchId;
            }
        } else {
            Batch lastBatch = getBatchFromFile(getBatchFile(lastBatchId));
            return lastBatch == null ? 0 : lastBatch.getBatchId();
        }
    }

    private void updateLastBatchId() {
        lastBatchId++;

        BufferedWriter bw = null;
        try {
            if (!batchIdFileSaver.exists()) {
                batchIdFileSaver.createNewFile();
            }

            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(batchIdFileSaver), Charset.forName("UTF-8")));
            bw.write(String.valueOf(lastBatchId));
            bw.flush();
        } catch (IOException e) {
            logger.warn("(updateLastBatchId) Exception while write to batchIdFileSaver file: ", e);
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                logger.warn("(updateLastBatchId) Exception while closing batchIdFileSaver file: ", e);
            }
        }
    }

    private void fillFirstFileId() {
        ArrayList<String> batchFiles = new ArrayList<String>();
        for (File file : path.listFiles()) {
            String fName = file.getName();
            if (fName.startsWith(BATCH_FILE_NAME)) {
                batchFiles.add(fName);
            }
        }
        Collections.sort(batchFiles);

        if (batchFiles.size() > 0) {
            firstFileId = getFileId(batchFiles.get(0));
        } else {
            firstFileId = lastBatchId;
        }
    }

    private Batch getBatchFromFile(File batchFile) {
        if (!batchFile.exists() || batchFile.length() == 0) return null;

        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(batchFile));

            byte[] nextBatchLength = new byte[BATCHSIZE_BYTES_COUNT];
            in.read(nextBatchLength);

            byte[] serializedBatch = new byte[ByteBuffer.wrap(nextBatchLength).getInt()];
            in.read(serializedBatch);

            return SerializationUtils.deserializeJsonGzip(serializedBatch);
        } catch (Exception e) {
            logger.warn("(getBatchFromFile) Exception while getting batch from file " + batchFile + ": ", e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                logger.warn("(getBatchFromFile) Exception while closing batch file " + batchFile + ": ", e);
            }
        }

        return null;
    }

    private File getBatchFile(int fileId) {
        return new File(path.getAbsolutePath() + "/" + BATCH_FILE_NAME + String.format(DIGITAL_FORMAT, fileId));
    }

    private int getFileId(String batchFileName) {
        return Integer.parseInt(batchFileName.substring(BATCH_FILE_NAME.length()));
    }

    @Override public Batch getNext() {
        synchronized (readLock) {

            Batch batch = getBatchFromFile(getBatchFile(firstFileId));

            return batch;
        }
    }

    @Override public void persist(List<Event> eventList) {
        synchronized (writeLock) {

            Batch batch = new Batch(lastBatchId, eventList);
            byte[] serializedBatch = SerializationUtils.serializeJsonGzip(batch);

            File file = getBatchFile(lastBatchId);

            BufferedOutputStream bos = null;
            try {
                if (!file.exists()) {
                    file.createNewFile();
                }

                bos = new BufferedOutputStream(new FileOutputStream(file));
                byte[] serializedBatchLength = ByteBuffer.allocate(BATCHSIZE_BYTES_COUNT).putInt(serializedBatch.length).array();
                bos.write(serializedBatchLength);
                bos.write(serializedBatch);
                bos.flush();

                updateLastBatchId();
            } catch (IOException e) {
                logger.warn("(Persist) Exception while persist batch: ", e);
            } finally {
                if (bos != null) {
                    try {
                        bos.close();
                    } catch (IOException e) {
                        logger.warn("(Persist) Exception while closing batch file: ", e);
                    }
                }
            }
        }
    }

    @Override public void remove() {
        synchronized (readLock) {
            getBatchFile(firstFileId).delete();
            firstFileId++;
        }
    }

}