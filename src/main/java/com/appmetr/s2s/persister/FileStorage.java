package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileStorage implements BatchStorage {
    private final static Logger log = LoggerFactory.getLogger(FileStorage.class);

    protected static final String BATCH_FILE_NAME = "batchFile#";
    protected static final String BATCH_FILE_GLOB_PATTERN = BATCH_FILE_NAME + "*";
    protected static final String DIGITAL_FORMAT = "%011d";

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected Queue<Long> fileIds;
    protected Path path;
    protected long lastBatchId;
    protected Path batchIdFile;

    public FileStorage(Path path) throws IOException {
        this.path = path;
        init();
    }

    @Override public boolean store(Collection<Action> actions, BatchFactory batchFactory) throws IOException {
        lock.writeLock().lock();
        try {
            final BinaryBatch binaryBatch = batchFactory.createBatch(actions, lastBatchId);
            final Path file = getBatchFile(lastBatchId);

            Files.write(file, binaryBatch.getBytes());

            fileIds.add(lastBatchId);
            updateLastBatchId();
        } finally {
            lock.writeLock().unlock();
        }

        return true;
    }

    @Override public BinaryBatch peek() throws InterruptedException, IOException {
        lock.readLock().lock();
        try {
            while (true) {
                final Long batchId = fileIds.peek();

                if (batchId == null) {
                    return null;
                }

                final byte[] bytes = getBatchFromFile(getBatchFile(batchId));
                if (bytes != null) {
                    return new BinaryBatch(batchId, bytes);
                }

                fileIds.remove();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void remove() throws IOException {
        lock.writeLock().lock();
        try {
            final Path batchFile = getBatchFile(fileIds.poll());
            if (batchFile != null) {
                Files.delete(batchFile);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void init() throws IOException {
        final List<Long> ids = new ArrayList<>();
        try (final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path, BATCH_FILE_GLOB_PATTERN)) {
            for (Path file : directoryStream) {
                ids.add(getFileId(file.getFileName().toString()));
            }
        }
        Collections.sort(ids);

        final long batchIdFileLength = Files.exists(batchIdFile) ? Files.size(batchIdFile) : 0;

        if (batchIdFileLength > 0) {
            final List<String> lines = Files.readAllLines(batchIdFile, StandardCharsets.UTF_8);
            lastBatchId = Integer.parseInt(lines.get(0));
        } else if (ids.size() > 0) {
            lastBatchId = ids.get(ids.size() - 1);
        } else {
            lastBatchId = 0;
        }

        log.debug("Init lastBatchId with {}", lastBatchId);

        fileIds = new ArrayDeque<>(ids);
        log.debug("Initialized {} batches.", ids.size());
    }


    protected Path getBatchFile(Long fileId) {
        return fileId == null ? null : path.toAbsolutePath().resolve(BATCH_FILE_NAME + String.format(DIGITAL_FORMAT, fileId));
    }

    protected long getFileId(String batchFileName) {
        return Long.parseLong(batchFileName.substring(BATCH_FILE_NAME.length()));
    }

    protected void updateLastBatchId() throws IOException {
        lastBatchId++;
        Files.write(batchIdFile, Collections.singleton(String.valueOf(lastBatchId)), StandardCharsets.UTF_8);
    }

    protected byte[] getBatchFromFile(Path batchFile) throws IOException {
        if (Files.notExists(batchFile) || Files.size(batchFile) == 0) {
            return null;
        }

        return Files.readAllBytes(batchFile);
    }
}
