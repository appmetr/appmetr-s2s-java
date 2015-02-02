package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileBatchPersister implements BatchPersister {
    private final static Logger logger = LoggerFactory.getLogger(FileBatchPersister.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private Queue<Integer> fileIds;

    private File path;
    private int lastBatchId;

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

        initPersistedFiles();
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
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                logger.warn("(updateLastBatchId) Exception while closing batchIdFileSaver file: ", e);
            }
        }
    }

    private void initPersistedFiles() {
        List<Integer> ids = new ArrayList<Integer>();
        for (File file : path.listFiles()) {
            String fName = file.getName();
            if (fName.startsWith(BATCH_FILE_NAME)) {
                ids.add(getFileId(file.getName()));
            }
        }

        Collections.sort(ids);

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
                    if (br != null) {
                        br.close();
                    }
                } catch (IOException e) {
                    logger.warn("Exception while closing batchIdFileSaver file: ", e);
                }

            }
            lastBatchId = batchId;
        } else if (ids.size() > 0) {
            lastBatchId = ids.get(ids.size() - 1);
        } else {
            lastBatchId = 0;
        }

        logger.info("Init lastBatchId with %s", lastBatchId);

        fileIds = new ArrayDeque<Integer>(ids);
    }

    private Batch getBatchFromFile(File batchFile) {
        if (!batchFile.exists() || batchFile.length() == 0) return null;

        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(batchFile));

            byte[] serializedBatch = new byte[in.available()];
            in.read(serializedBatch);

            return SerializationUtils.deserializeJsonGzip(serializedBatch);
        } catch (Exception e) {
            logger.warn("(getBatchFromFile) Exception while getting batch from file " + batchFile + ": ", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                logger.warn("(getBatchFromFile) Exception while closing batch file " + batchFile + ": ", e);
            }
        }

        return null;
    }

    private File getBatchFile(Integer fileId) {
        return fileId == null ? null : new File(path.getAbsolutePath() + "/" + BATCH_FILE_NAME + String.format(DIGITAL_FORMAT, fileId));
    }

    private int getFileId(String batchFileName) {
        return Integer.parseInt(batchFileName.substring(BATCH_FILE_NAME.length()));
    }

    @Override public Batch getNext() {
        lock.readLock().lock();
        try {
            Integer batchId = fileIds.peek();

            if (batchId == null) {
                return null;
            }

            return getBatchFromFile(getBatchFile(batchId));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void persist(List<Action> actionList) {
        lock.writeLock().lock();
        try {

            Batch batch = new Batch(lastBatchId, actionList);
            byte[] serializedBatch = SerializationUtils.serializeJsonGzip(batch, true);

            File file = getBatchFile(lastBatchId);

            BufferedOutputStream bos = null;
            try {
                if (!file.exists()) {
                    file.createNewFile();
                }

                bos = new BufferedOutputStream(new FileOutputStream(file));
                bos.write(serializedBatch);
                bos.flush();

                fileIds.add(lastBatchId);
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
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void remove() {
        lock.writeLock().lock();
        try {
            getBatchFile(fileIds.poll()).delete();
        } finally {
            lock.writeLock().unlock();
        }
    }

}