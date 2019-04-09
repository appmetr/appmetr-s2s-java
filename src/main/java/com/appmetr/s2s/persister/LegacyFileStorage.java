package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * Only for migration purpose from old file format
 */
public class LegacyFileStorage extends FileStorage {
    private final static Logger log = LoggerFactory.getLogger(LegacyFileStorage.class);

    protected static final String BATCH_FILE_NAME_LEGACY_PREFIX = BATCH_FILE + "#";

    public LegacyFileStorage(Path path) throws IOException {
        super(path);
    }

    @Override protected byte[] getBatchFromFile(Path batchFile) throws IOException {
        final byte[] bytes = super.getBatchFromFile(batchFile);
        if (bytes != null) {
            return bytes;
        }

        final Path legacyBatchFile = legacyFile(batchFile);
        if (Files.notExists(legacyBatchFile) || Files.size(legacyBatchFile) == 0) {
            return null;
        }

        log.info("Found legacy batch file {}", legacyBatchFile);

        final byte[] legacyBytes = Files.readAllBytes(legacyBatchFile);
        final Batch batch = SerializationUtils.deserializeJsonGzip(legacyBytes);

        return SerializationUtils.serializeJsonGzip(batch, false);
    }

    @Override protected void tryDeleteFile(Path batchFile) throws IOException {
        try {
            super.tryDeleteFile(batchFile);
        } catch (NoSuchFileException e) {
            super.tryDeleteFile(legacyFile(batchFile));
        }
    }

    protected Path legacyFile(Path batchFile) {
        final long batchId = batchId(batchFile.getFileName().toString());
        return batchFilePath(batchId, BATCH_FILE_NAME_LEGACY_PREFIX);
    }
}
