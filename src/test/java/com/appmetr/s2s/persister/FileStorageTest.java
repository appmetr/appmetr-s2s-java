package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.AfterEach;
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

    @AfterEach
    void tearDown() {
    }

    @Test
    void store() throws IOException, InterruptedException {
        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch1 = fileStorage.peek();
                assertEquals(0, binaryBatch1.getBatchId());
                fileStorage.remove();

                final BinaryBatch binaryBatch2 = fileStorage.peek();
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

        assertTrue(fileStorage.fileIds.isEmpty());
    }

    @Test
    void peek() {
    }

    @Test
    void remove() {
    }
}