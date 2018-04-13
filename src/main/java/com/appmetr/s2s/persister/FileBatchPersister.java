package com.appmetr.s2s.persister;

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

    public static final int BYTES_IN_MB = 1024 * 1024;
    public static final int REBATCH_THRESHOLD_ITEM_COUNT = 1000;
    public static final int REBATCH_THRESHOLD_FILE_SIZE = 1 * BYTES_IN_MB;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected Queue<Long> fileIds;

    protected Path path;
    protected long lastBatchId;

    protected final Path batchIdFileSaver;
    protected final String BATCH_FILE_NAME = "batchFile#";
    protected final String BATCH_FILE_GLOB_PATTERN = BATCH_FILE_NAME + "*";
    protected final String DIGITAL_FORMAT = "%011d";
    protected String serverId = UUID.randomUUID().toString();

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
    }

    protected void updateLastBatchId() {
        lastBatchId++;

        try {
            Files.write(batchIdFileSaver, Collections.singleton(String.valueOf(lastBatchId)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("(updateLastBatchId) Exception while write to batchIdFileSaver file:", e);
        }
    }

    protected void initPersistedFiles() {
        final List<Long> ids = new ArrayList<>();
        try {
            for (Path file : Files.newDirectoryStream(path, BATCH_FILE_GLOB_PATTERN)) {
                ids.add(getFileId(file.getFileName().toString()));
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

        log.debug("Init lastBatchId with {}", lastBatchId);

        fileIds = new ArrayDeque<>(ids);
        log.debug("Initialized {} batches.", ids.size());
    }

    protected Batch getBatchFromFile(Path batchFile) {
        try {
            if (Files.notExists(batchFile) || Files.size(batchFile) == 0) {
                return null;
            }

            final byte[] serializedBatch = Files.readAllBytes(batchFile);
            return SerializationUtils.deserializeJsonGzip(serializedBatch);
        } catch (IOException e) {
            log.error("(getBatchFromFile) Exception while getting batch from file {}:", batchFile, e);
            return null;
        }
    }

    protected Path getBatchFile(Long fileId) {
        return fileId == null ? null : path.toAbsolutePath().resolve(BATCH_FILE_NAME + String.format(DIGITAL_FORMAT, fileId));
    }

    protected long getFileId(String batchFileName) {
        return Long.parseLong(batchFileName.substring(BATCH_FILE_NAME.length()));
    }

    @Override public Batch getNext() {
        lock.readLock().lock();
        try {

            while (true) {
                final Long batchId = fileIds.peek();

                if (batchId == null) {
                    return null;
                }

                final Batch batch = getBatchFromFile(getBatchFile(batchId));
                if (batch != null) {
                    return batch;
                }

                fileIds.remove();
            }

        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public boolean persist(List<Action> actionList) {
        lock.writeLock().lock();
        try {

            final Batch batch = new Batch(serverId, lastBatchId, actionList);
            final byte[] serializedBatch = SerializationUtils.serializeJsonGzip(batch, true);

            final Path file = getBatchFile(lastBatchId);

            try {
                Files.write(file, serializedBatch);

                fileIds.add(lastBatchId);
                updateLastBatchId();
            } catch (IOException e) {
                log.error("(Persist) Exception while persist batch:", e);
            }

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void remove() {
        lock.writeLock().lock();
        try {
            final Path batchFile = getBatchFile(fileIds.poll());
            if (batchFile != null) {
                Files.delete(batchFile);
            }
        } catch (IOException e) {
            log.error("", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void setServerId(String serverId) {
        this.serverId = serverId;
    }
}
