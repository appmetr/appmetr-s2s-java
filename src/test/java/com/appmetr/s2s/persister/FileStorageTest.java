package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class FileStorageTest {

    static BatchFactory batchFactory = (actions, batchId) -> new BinaryBatch(batchId, new byte[1]);

    FileStorage fileStorage;

    @BeforeEach
    void setUp(@TempDir Path path) throws IOException {
        System.out.println(path);
        fileStorage = new FileStorage(path);
    }

    @Test
    void storeAndPeekConcurrently() throws IOException, InterruptedException {
        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch1 = fileStorage.get();
                assertEquals(0, binaryBatch1.getBatchId());
                fileStorage.remove();

                final BinaryBatch binaryBatch2 = fileStorage.get();
                assertEquals(1, binaryBatch2.getBatchId());
                fileStorage.remove();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });

        Throwable[] throwables = new Throwable[1];
        consumerThread.setUncaughtExceptionHandler((t, e) -> throwables[0] = e);
        consumerThread.start();

        assertTrue(fileStorage.store(Collections.singleton(new Event("test0")), batchFactory));
        assertTrue(fileStorage.store(Collections.singleton(new Event("test1")), batchFactory));

        consumerThread.join();
        if (throwables[0] != null) {
            fail(throwables[0]);
        }

        assertTrue(fileStorage.isEmpty());
    }

    @Test
    void restore() throws IOException, InterruptedException {
        assertTrue(fileStorage.store(Collections.singleton(new Event("test0")), batchFactory));
        assertTrue(fileStorage.store(Collections.singleton(new Event("test1")), batchFactory));

        final FileStorage otherStorage = new FileStorage(this.fileStorage.path);
        final BinaryBatch binaryBatch1 = otherStorage.get();
        assertEquals(0, binaryBatch1.getBatchId());
        otherStorage.remove();

        final BinaryBatch binaryBatch2 = otherStorage.get();
        assertEquals(1, binaryBatch2.getBatchId());
        otherStorage.remove();

        assertTrue(otherStorage.isEmpty());
    }
}
