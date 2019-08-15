package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class FileStorage implements BatchStorage {
    private final static Logger log = LoggerFactory.getLogger(FileStorage.class);

    protected static final String BATCH_FILE = "batchFile";
    protected static final String BATCH_FILE_NAME_PREFIX = BATCH_FILE + "-";
    protected static final String BATCH_FILE_GLOB_PATTERN = BATCH_FILE + "*";
    protected static final String DIGITAL_FORMAT = "%011d";
    protected static final String LAST_BATCH_ID_FILE_NAME = "lastBatchId";

    protected Queue<Long> fileIds;
    protected Path path;
    protected long lastBatchId;
    protected Path batchIdFile;

    public FileStorage(Path path) throws IOException {
        this.path = path;
        init();
    }

    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws IOException {
        final BinaryBatch binaryBatch = batchFactory.createBatch(actions, lastBatchId);
        return store(binaryBatch);
    }

    protected synchronized boolean store(BinaryBatch binaryBatch) throws IOException {
        final Path file = batchFilePath(lastBatchId);

        Files.write(file, binaryBatch.getBytes());

        fileIds.add(lastBatchId);
        updateLastBatchId();

        notify();

        return true;
    }

    @Override public synchronized BinaryBatch peek() throws InterruptedException, IOException {
        while (true) {
            final Long batchId = fileIds.peek();
            if (batchId == null) {
                wait();
                continue;
            }

            final Path batchFile = batchFilePath(batchId);
            final byte[] bytes = getBatchFromFile(batchFile);
            if (bytes != null) {
                return new BinaryBatch(batchId, bytes);
            }

            log.warn("Batch file {} is missing or empty", batchFile);

            fileIds.remove();
        }
    }

    @Override public synchronized void remove() throws IOException {
        final Path batchFile = batchFilePath(fileIds.poll());
        if (batchFile != null) {
            tryDeleteFile(batchFile);
        }
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return fileIds.isEmpty();
    }

    protected void init() throws IOException {
        if (Files.notExists(path)) {
            Files.createDirectories(path);
        }
        if (!Files.isDirectory(path)) {
            path = path.getParent();
        }

        batchIdFile = path.toAbsolutePath().resolve(LAST_BATCH_ID_FILE_NAME);

        final List<Long> ids = new ArrayList<>();
        try (final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path, BATCH_FILE_GLOB_PATTERN)) {
            for (Path file : directoryStream) {
                ids.add(batchId(file.getFileName().toString()));
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

    private Path batchFilePath(Long fileId) {
        return batchFilePath(fileId, BATCH_FILE_NAME_PREFIX);
    }

    protected Path batchFilePath(Long fileId, String namePrefix) {
        return fileId == null ? null : path.toAbsolutePath().resolve(namePrefix + String.format(DIGITAL_FORMAT, fileId));
    }

    protected long batchId(String batchFileName) {
        return Long.parseLong(batchFileName.substring(BATCH_FILE_NAME_PREFIX.length()));
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

    protected void tryDeleteFile(Path batchFile) throws IOException {
        Files.delete(batchFile);
    }
}
