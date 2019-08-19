package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

class HeapStorageTest {

    static BatchFactory batchFactory = (actions, batchId) -> new BinaryBatch(batchId, new byte[1]);

    HeapStorage heapStorage = new HeapStorage();

    @BeforeEach
    void setUp() {
        heapStorage.setClock(Clock.fixed(Instant.ofEpochMilli(1), ZoneOffset.UTC));
    }

    @Test
    void blocksForever() throws InterruptedException {
        heapStorage.setMaxBytes(1);
        assertTrue(heapStorage.store(Collections.singleton(new Event("test1")), batchFactory));
        assertThrows(AssertionFailedError.class, () -> assertTimeoutPreemptively(ofMillis(1), () -> {
            heapStorage.store(Collections.singleton(new Event("test2")), batchFactory);
        }));
        assertEquals(1, heapStorage.batchesQueue.size());
    }

    @Test
    void storeAndPeekConcurrently() throws InterruptedException {
        heapStorage.setMaxBytes(1);

        Throwable[] throwables = new Throwable[2];

        final Thread producerThread = new Thread(() -> {
            try {
                assertTrue(heapStorage.store(Collections.singleton(new Event("test1")), batchFactory));
                assertTrue(heapStorage.store(Collections.singleton(new Event("test2")), batchFactory));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
        producerThread.setUncaughtExceptionHandler((t, e) -> throwables[0] = e);
        producerThread.start();

        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch1 = heapStorage.get();
                assertEquals(1, binaryBatch1.getBatchId());
                heapStorage.remove();

                final BinaryBatch binaryBatch2 = heapStorage.get();
                assertEquals(2, binaryBatch2.getBatchId());
                heapStorage.remove();
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

        assertTrue(heapStorage.isEmpty());
    }
}
