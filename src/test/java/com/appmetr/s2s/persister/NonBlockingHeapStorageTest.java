package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class NonBlockingHeapStorageTest {

    static BatchFactory batchFactory = (actions, batchId) -> new BinaryBatch(batchId, new byte[2]);

    NonBlockingHeapStorage nonBlockingHeapStorage = new NonBlockingHeapStorage();

    @BeforeEach
    void setUp() {
        nonBlockingHeapStorage.setClock(Clock.fixed(Instant.ofEpochMilli(1), ZoneOffset.UTC));
    }

    @Test
    void producerFirst() throws InterruptedException {
        assertTrue(nonBlockingHeapStorage.store(Collections.singleton(new Event("test")), batchFactory));

        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch = nonBlockingHeapStorage.peek();
                assertEquals(1, binaryBatch.getBatchId());
                nonBlockingHeapStorage.remove();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });

        Throwable[] throwables = new Throwable[1];
        consumerThread.setUncaughtExceptionHandler((t, e) -> throwables[0] = e);
        consumerThread.start();
        consumerThread.join();
        if (throwables[0] != null) {
            fail(throwables[0]);
        }

        assertTrue(nonBlockingHeapStorage.batchesQueue.isEmpty());
    }

    @Test
    void consumerFirst() throws InterruptedException {
        final Thread consumerThread = new Thread(() -> {
            try {
                final BinaryBatch binaryBatch = nonBlockingHeapStorage.peek();
                assertEquals(1, binaryBatch.getBatchId());
                nonBlockingHeapStorage.remove();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });

        Throwable[] throwables = new Throwable[1];
        consumerThread.setUncaughtExceptionHandler((t, e) -> throwables[0] = e);
        consumerThread.start();

        Thread.sleep(1);
        assertTrue(nonBlockingHeapStorage.store(Collections.singleton(new Event("test")), batchFactory));

        consumerThread.join();
        if (throwables[0] != null) {
            fail(throwables[0]);
        }

        assertTrue(nonBlockingHeapStorage.batchesQueue.isEmpty());
    }

    @Test
    void drop() throws InterruptedException {
        nonBlockingHeapStorage.setMaxBytes(1);

        assertFalse(nonBlockingHeapStorage.store(Collections.singleton(new Event("test")), batchFactory));
        assertTrue(nonBlockingHeapStorage.batchesQueue.isEmpty());
    }
}
