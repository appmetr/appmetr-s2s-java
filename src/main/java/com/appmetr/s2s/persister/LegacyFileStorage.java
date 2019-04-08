package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
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

        final long batchId = batchId(batchFile.getFileName().toString());
        final Path legacyBatchFile = batchFilePath(batchId, BATCH_FILE_NAME_LEGACY_PREFIX);
        if (Files.notExists(legacyBatchFile) || Files.size(legacyBatchFile) == 0) {
            return null;
        }

        log.info("Found legacy batch file {}", batchFile);

        final byte[] legacyBytes = Files.readAllBytes(batchFile);
        final Batch batch = SerializationUtils.deserializeJsonGzip(legacyBytes);

        return SerializationUtils.serializeJsonGzip(batch, false);
    }
}
