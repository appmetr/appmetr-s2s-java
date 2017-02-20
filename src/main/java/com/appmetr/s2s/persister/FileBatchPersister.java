package com.appmetr.s2s.persister;

import com.appmetr.s2s.AppMetr;
import com.appmetr.s2s.Batch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileBatchPersister implements BatchPersister {
    private final static Logger log = LoggerFactory.getLogger(FileBatchPersister.class);
    private static final int BYTES_IN_MB = 1024 * 1024;
    public static final int REBATCH_THRESHOLD_ITEM_COUNT = 1000;
    public static final int REBATCH_THRESHOLD_FILE_SIZE = 1 * BYTES_IN_MB;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private Queue<Integer> fileIds;

    private Path path;
    private int lastBatchId;

    private final Path batchIdFileSaver;
    private final String BATCH_FILE_NAME = "batchFile#";
    private final String DIGITAL_FORMAT = "%011d";

    public FileBatchPersister(String filePath) {
        path = Paths.get(filePath);

        if (Files.notExists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                log.error("", e);
            }
        }

        if (!Files.isDirectory(path)) {
            path = path.getParent();
        }

        batchIdFileSaver = path.toAbsolutePath().resolve("lastBatchId");

        initPersistedFiles();
        rebatch();
    }

    private void updateLastBatchId() {
        lastBatchId++;

        try {
            Files.write(batchIdFileSaver, Collections.singleton(String.valueOf(lastBatchId)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("(updateLastBatchId) Exception while write to batchIdFileSaver file:", e);
        }
    }

    private void initPersistedFiles() {
        List<Integer> ids = new ArrayList<>();
        try {
            for (Path file : Files.newDirectoryStream(path)) {
                final String fileName = file.getFileName().toString();
                if (fileName.startsWith(BATCH_FILE_NAME)) {
                    ids.add(getFileId(fileName));
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }

        Collections.sort(ids);

        long batchIdFileLength = 0;
        try {
            batchIdFileLength = Files.exists(batchIdFileSaver) ? Files.size(batchIdFileSaver) : 0;
        } catch (IOException e) {
            log.error("", e);
        }

        if (batchIdFileLength > 0) {
            try {
                final List<String> lines = Files.readAllLines(batchIdFileSaver, StandardCharsets.UTF_8);
                lastBatchId = Integer.parseInt(lines.get(0));
            } catch (IOException e) {
                log.error("Exception while reading from batchIdFileSaver: ", e);
            }
        } else if (ids.size() > 0) {
            lastBatchId = ids.get(ids.size() - 1);
        } else {
            lastBatchId = 0;
        }

        log.info("Init lastBatchId with {}", lastBatchId);

        fileIds = new ArrayDeque<>(ids);
        log.info("Initialized {} batches.", ids.size());
    }

    public void rebatch() {
        try {
            lock.writeLock().lock();

            final long rebatchingStart = System.currentTimeMillis();

            boolean rebatchNeeded = false;
            for (Path batchFile : Files.newDirectoryStream(path)) {
                if (Files.size(batchFile) > REBATCH_THRESHOLD_FILE_SIZE) {
                    rebatchNeeded = true;
                    log.info("Rebatch condition detected");
                    break;
                }
            }

            if (rebatchNeeded) {
                log.info("Rebatching starting.");
                final Queue<Integer> originalBatches = new ArrayDeque<>(fileIds);
                Integer newBatchId = originalBatches.peek();

                final Path originalPath = path.resolve("original");
                Files.createDirectories(originalPath);

                for (Path batchFile : Files.newDirectoryStream(path)) {
                    Files.move(batchFile, originalPath.resolve(batchFile.getFileName()));
                }

                for (Path file : Files.newDirectoryStream(originalPath)) {
                    final long batchRegroupStart = System.currentTimeMillis();

                    if (file.getFileName().toString().equals("lastBatchId")) {
                        continue;
                    }

                    final Batch batch = getBatchFromFile(file);
                    if (batch == null) {
                        continue;
                    }

                    int regroupedCount = 0;
                    if (batch.getBatch().size() >= REBATCH_THRESHOLD_ITEM_COUNT
                            || Files.size(file) >= REBATCH_THRESHOLD_FILE_SIZE) {

                        List<Action> rebatchedActions = new ArrayList<>(REBATCH_THRESHOLD_ITEM_COUNT);
                        for (Action action : batch.getBatch()) {
                            if (rebatchedActions.size() >= REBATCH_THRESHOLD_ITEM_COUNT) {
                                persist(rebatchedActions);
                                regroupedCount++;
                                rebatchedActions = new ArrayList<>(REBATCH_THRESHOLD_ITEM_COUNT);
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
                    log.info("Batch {} regrouped in to {}. Took {} ms.", batch.getBatchId(), regroupedCount, batchRegroupEnd - batchRegroupStart);
                }
                final long rebatchingEnd = System.currentTimeMillis();

                log.info("Rebatching finished. Rebatched {} to {} batches. Took {} ms", originalBatches.size(), lastBatchId - newBatchId, rebatchingEnd-rebatchingStart);

                //Reload all data after rebatching
                initPersistedFiles();
            } else {
                log.debug("No rebatching needed. Starting");
            }

        } catch (IOException e) {
            log.error("", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Batch getBatchFromFile(Path batchFile) {
        try {
            if (Files.notExists(batchFile) || Files.size(batchFile) == 0) {
                return null;
            }

            byte[] serializedBatch = Files.readAllBytes(batchFile);
            return SerializationUtils.deserializeJsonGzip(serializedBatch);
        } catch (IOException e) {
            log.error("(getBatchFromFile) Exception while getting batch from file {}:", batchFile, e);
            return null;
        }
    }

    private Path getBatchFile(Integer fileId) {
        return fileId == null ? null : path.toAbsolutePath().resolve(BATCH_FILE_NAME + String.format(DIGITAL_FORMAT, fileId));
    }

    private int getFileId(String batchFileName) {
        return Integer.parseInt(batchFileName.substring(BATCH_FILE_NAME.length()));
    }

    @Override public Batch getNext() {
        lock.readLock().lock();
        try {
            Batch batch;

            while (true) {
                Integer batchId = fileIds.peek();

                if (batchId == null) {
                    return null;
                }

                batch = getBatchFromFile(getBatchFile(batchId));
                if (batch != null) {
                    break;
                }

                fileIds.remove();
            }

            return batch;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void persist(List<Action> actionList) {
        lock.writeLock().lock();
        try {

            Batch batch = new Batch(AppMetr.SERVER_ID, lastBatchId, actionList);
            byte[] serializedBatch = SerializationUtils.serializeJsonGzip(batch, true);

            Path file = getBatchFile(lastBatchId);

            try {
                Files.write(file, serializedBatch);

                fileIds.add(lastBatchId);
                updateLastBatchId();
            } catch (IOException e) {
                log.error("(Persist) Exception while persist batch:", e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void remove() {
        lock.writeLock().lock();
        try {
            Files.delete(getBatchFile(fileIds.poll()));
        } catch (IOException e) {
            log.error("", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
