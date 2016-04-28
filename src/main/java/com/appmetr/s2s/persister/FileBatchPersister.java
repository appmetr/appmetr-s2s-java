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
    private static final int BYTES_IN_MB = 1024 * 1024;
    public static final int REBATCH_THRESHOLD_ITEM_COUNT = 1000;
    public static final int REBATCH_THRESHOLD_FILE_SIZE = 1 * BYTES_IN_MB;

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
        rebatch();
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
            logger.error("(updateLastBatchId) Exception while write to batchIdFileSaver file: ", e);
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
                logger.error("Exception while reading from batchIdFileSaver: ", e);
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

        logger.info(String.format("Init lastBatchId with %s", lastBatchId));

        fileIds = new ArrayDeque<Integer>(ids);
        logger.info("Initialized "+ids.size()+" batches.");
    }

    public void rebatch() {
        try {
            lock.writeLock().lock();

            final long rebatchingStart = System.currentTimeMillis();

            final File[] batchFiles = path.listFiles();
            boolean rebatchNeeded = false;
            for (File batchFile : batchFiles) {
                if (batchFile.length() > REBATCH_THRESHOLD_FILE_SIZE) {
                    rebatchNeeded = true;
                    logger.warn("Rebatch condition detected");
                    break;
                }
            }

            if (rebatchNeeded) {
                logger.info("Rebatching starting.");
                final Queue<Integer> originalBatches = new ArrayDeque<Integer>(fileIds);
                Integer newBatchId = originalBatches.peek();


                final File originalPath = new File(path.getPath() + "/original");
                originalPath.mkdirs();


                for (File batchFile : batchFiles) {
                    batchFile.renameTo(new File(originalPath, batchFile.getName()));
                }

                for (File file : originalPath.listFiles()) {
                    final long batchRegroupStart = System.currentTimeMillis();

                    if(file.getName().equals("lastBatchId")) continue;

                    final Batch batch = getBatchFromFile(file);
                    int regroupedCount=0;

                    if (batch.getBatch().size() >= REBATCH_THRESHOLD_ITEM_COUNT
                            || file.length() >= REBATCH_THRESHOLD_FILE_SIZE) {

                        List<Action> rebatchedActions = new ArrayList<Action>(REBATCH_THRESHOLD_ITEM_COUNT);
                        for (Action action : batch.getBatch()) {
                            if (rebatchedActions.size() >= REBATCH_THRESHOLD_ITEM_COUNT) {
                                persist(rebatchedActions);
                                regroupedCount++;
                                rebatchedActions = new ArrayList<Action>(REBATCH_THRESHOLD_ITEM_COUNT);
                            }
                            rebatchedActions.add(action);
                        }

                        if (rebatchedActions.size() > 0) {
                            persist(rebatchedActions);
                            regroupedCount++;
                        }
                    } else {
                        persist(batch.getBatch());
                        regroupedCount++;
                    }

                    final long batchRegroupEnd = System.currentTimeMillis();
                    logger.info(String.format("Batch %d regrouped in to %d. Took %d ms.", batch.getBatchId(), regroupedCount, batchRegroupEnd - batchRegroupStart));
                }
                final long rebatchingEnd = System.currentTimeMillis();

                logger.info(String.format("Rebatching finished. Rebatched %d to %d batches. Took %d ms", originalBatches.size(), lastBatchId - newBatchId, rebatchingEnd-rebatchingStart));

                //Reload all data after rebatching
                initPersistedFiles();
            } else {
                logger.debug("No rebatching needed. Starting");
            }

        }finally {
            lock.writeLock().unlock();
        }
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
            logger.error("(getBatchFromFile) Exception while getting batch from file " + batchFile + ": ", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error("(getBatchFromFile) Exception while closing batch file " + batchFile + ": ", e);
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
                logger.error("(Persist) Exception while persist batch: ", e);
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