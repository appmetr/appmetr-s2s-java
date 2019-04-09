package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class LegacyFileStorageTest {

    static BatchFactory batchFactory = (actions, batchId) -> new BinaryBatch(batchId, new byte[1]);

    LegacyFileStorage legacyFileStorage;

    @BeforeEach
    void setUp(@TempDir Path path) throws IOException {
    }

    @Test
    void readFromLegacyFiles(@TempDir Path path) throws IOException, InterruptedException {
        System.out.println(path);

        createBatch(path, 0);
        createBatch(path, 1);
        Path batchIdFile = path.resolve(LegacyFileStorage.LAST_BATCH_ID_FILE_NAME);
        Files.write(batchIdFile, Collections.singleton(String.valueOf(2)), StandardCharsets.UTF_8);

        legacyFileStorage = new LegacyFileStorage(path);
        assertTrue(legacyFileStorage.store(Collections.singleton(new Event("test2")), batchFactory));

        final LegacyFileStorage otherStorage = new LegacyFileStorage(path);
        final BinaryBatch binaryBatch1 = otherStorage.peek();
        assertEquals(0, binaryBatch1.getBatchId());
        otherStorage.remove();
        final BinaryBatch binaryBatch2 = otherStorage.peek();
        assertEquals(1, binaryBatch2.getBatchId());
        otherStorage.remove();
        final BinaryBatch binaryBatch3 = otherStorage.peek();
        assertEquals(2, binaryBatch3.getBatchId());
        otherStorage.remove();

        assertTrue(otherStorage.fileIds.isEmpty());
    }

    void createBatch(Path path, long batchId) throws IOException {
        final Batch batch = new Batch("s1", 0, Collections.singleton(new Event("test" + batchId)));
        final byte[] serializedBatch = SerializationUtils.serializeJsonGzip(batch, true);
        final Path file =  path.resolve(LegacyFileStorage.BATCH_FILE_NAME_LEGACY_PREFIX + String.format(LegacyFileStorage.DIGITAL_FORMAT, batchId));
        Files.write(file, serializedBatch);
    }
}
