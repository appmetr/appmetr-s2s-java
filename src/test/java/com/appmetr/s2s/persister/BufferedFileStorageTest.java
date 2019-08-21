package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

class BufferedFileStorageTest {

    static BatchFactory batchFactory = (actions, batchId) -> new BinaryBatch(batchId, new byte[2]);

    HeapStorage heapStorage = new HeapStorage();
    NonBlockingHeapStorage nonBlockingHeapStorage = new NonBlockingHeapStorage();
    FileStorage fileStorage;
    BufferedFileStorage bufferedFileStorage;

    @BeforeEach
    void setUp(@TempDir Path path) throws IOException {
        System.out.println(path);
        fileStorage = new FileStorage(path);
    }

    @Test
    void blocksForever() {
        heapStorage.setMaxBytes(1);
        bufferedFileStorage = new BufferedFileStorage(fileStorage, heapStorage);
        assertThrows(AssertionFailedError.class, () -> assertTimeoutPreemptively(ofMillis(1), () -> {
            bufferedFileStorage.store(Collections.singleton(new Event("test1")), batchFactory);
        }));
        assertTrue(heapStorage.isEmpty());
    }

    @Test
    void drop() throws InterruptedException, IOException {
        nonBlockingHeapStorage.setMaxBytes(1);
        bufferedFileStorage = new BufferedFileStorage(fileStorage, nonBlockingHeapStorage);

        assertFalse(bufferedFileStorage.store(Collections.singleton(new Event("test")), batchFactory));
        assertTrue(nonBlockingHeapStorage.batchesQueue.isEmpty());
    }

    @Test
    void storeAndPeekConcurrently() throws InterruptedException {
        heapStorage.setMaxBytes(2);
        bufferedFileStorage = new BufferedFileStorage(fileStorage, heapStorage);

        Throwable[] throwables = new Throwable[2];

        final Thread producerThread = new Thread(() -> {
            try {
                assertTrue(bufferedFileStorage.store(Collections.singleton(new Event("test1")), batchFactory));
                assertTrue(bufferedFileStorage.store(Collections.singleton(new Event("test2")), batchFactory));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
        producerThread.setUncaughtExceptionHandler((t, e) -> throwables[0] = e);
        producerThread.start();

        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch1 = bufferedFileStorage.get();
                assertEquals(0, binaryBatch1.getBatchId());
                bufferedFileStorage.remove();

                final BinaryBatch binaryBatch2 = bufferedFileStorage.get();
                assertEquals(1, binaryBatch2.getBatchId());
                bufferedFileStorage.remove();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
        consumerThread.setUncaughtExceptionHandler((t, e) -> {
            throwables[1] = e;
            producerThread.interrupt();
        });
        consumerThread.start();

        producerThread.join();
        if (throwables[0] != null) {
            fail(String.valueOf(throwables[1]), throwables[0]);
        }
        consumerThread.join();
        if (throwables[1] != null) {
            fail(throwables[1]);
        }

        assertTrue(bufferedFileStorage.isEmpty());
    }
}
