package com.appmetr.s2s;


import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.persister.BatchFactory;
import com.appmetr.s2s.persister.GzippedJsonBatchFactoryTest;
import com.appmetr.s2s.persister.HeapStorage;
import com.appmetr.s2s.sender.BatchSender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class AppMetrTest {

    static String token = "testToken";
    static String url = "testUrl";

    AppMetr appMetr = new AppMetr(token, url);

    @BeforeEach
    void setUp() {
        appMetr.setServerId("s1");
    }

    @Test
    void sendManually() throws Exception {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);

        appMetr.setBatchSender(mockSender);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();

        ArgumentCaptor<byte[]> batch1 = ArgumentCaptor.forClass(byte[].class);
        verify(mockSender, timeout(100)).send(eq(url), eq(token), batch1.capture());
        final JsonNode batchNode = GzippedJsonBatchFactoryTest.decompress(batch1.getValue());
        assertEquals("s1", batchNode.get("serverId").asText());
        final ArrayNode events = (ArrayNode) batchNode.get("batch");
        assertTrue(events.isArray());
        assertEquals(1, events.size());

        assertEquals("trackEvent", events.get(0).get("action").asText());
        assertEquals("test1", events.get(0).get("event").asText());

        appMetr.hardStop();
    }

    @Test
    void storeByBatchActions() throws Exception {
        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(NothingBatchSender.instance);
        appMetr.setBatchStorage(testStorage);
        appMetr.setMaxBatchActions(1);
        appMetr.start();

        final Event event1 = new Event("test1");
        final Event event2 = new Event("test2");
        assertTrue(appMetr.track(event1));
        assertTrue(appMetr.track(event2));

        assertEquals(1, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event1), testStorage.storeCalls.get(0));

        appMetr.hardStop();

        assertEquals(2, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event2), testStorage.storeCalls.get(1));
    }

    @Test
    void storeByBatchBytes() throws Exception {
        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(NothingBatchSender.instance);
        appMetr.setBatchStorage(testStorage);
        appMetr.setMaxBatchBytes(1);
        appMetr.start();

        final Event event1 = new Event("test1");
        final Event event2 = new Event("test2");
        assertTrue(appMetr.track(event1));
        assertTrue(appMetr.track(event2));

        assertEquals(1, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event1), testStorage.storeCalls.get(0));

        appMetr.hardStop();

        assertEquals(2, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event2), testStorage.storeCalls.get(1));
    }

    @Test
    void storeByTime() throws Exception {
        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(NothingBatchSender.instance);
        appMetr.setBatchStorage(testStorage);
        appMetr.setFlushPeriod(Duration.ofSeconds(1));
        appMetr.setClock(Clock.fixed(Instant.ofEpochSecond(1), ZoneOffset.UTC));
        appMetr.start();

        final Event event1 = new Event("test1");
        final Event event2 = new Event("test2");
        assertTrue(appMetr.track(event1));
        assertEquals(0, testStorage.storeCalls.size());

        appMetr.setClock(Clock.fixed(Instant.ofEpochSecond(2), ZoneOffset.UTC));

        assertTrue(appMetr.track(event2));

        assertEquals(1, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event1), testStorage.storeCalls.get(0));

        appMetr.hardStop();

        assertEquals(2, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event2), testStorage.storeCalls.get(1));
    }

    @Test
    void storeDiscarded() throws Exception {
        final DiscardStorage testStorage = new DiscardStorage();

        appMetr.setBatchSender(NothingBatchSender.instance);
        appMetr.setBatchStorage(testStorage);
        appMetr.setMaxBatchActions(1);
        appMetr.start();

        final Event event1 = new Event("test1");
        final Event event2 = new Event("test2");
        assertTrue(appMetr.track(event1));
        assertFalse(appMetr.track(event2));

        appMetr.hardStop();
    }

    @Test
    void senderSuccess() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);
        when(mockSender.send(eq(url), eq(token), any())).thenReturn(true);

        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();

        verify(mockSender, timeout(100)).send(eq(url), eq(token), any());

        appMetr.stop();

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void senderSuccessSeveralBatches() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);
        when(mockSender.send(eq(url), eq(token), any())).thenReturn(true);

        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));
        appMetr.flush();
        assertTrue(appMetr.track(new Event("test2")));
        appMetr.stop();

        verify(mockSender, times(2)).send(eq(url), eq(token), any());

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void persistentStorageAndStop() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);
        when(mockSender.send(eq(url), eq(token), any())).thenReturn(true);

        final TestStorage testStorage = new TestPersistentStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));
        appMetr.flush();
        assertTrue(appMetr.track(new Event("test2")));
        appMetr.stop();

        verify(mockSender).send(eq(url), eq(token), any());

        assertEquals(1, testStorage.getBathesQueue().size());
    }

    @Test
    void persistentStorageAndSoftStop() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);
        when(mockSender.send(eq(url), eq(token), any())).thenReturn(true);

        final TestStorage testStorage = new TestPersistentStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));
        appMetr.flush();
        assertTrue(appMetr.track(new Event("test2")));
        appMetr.softStop();

        verify(mockSender, times(2)).send(eq(url), eq(token), any());

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void asyncAppmetrPersistentStorageAndSoftStop() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);
        when(mockSender.send(eq(url), eq(token), any())).thenReturn(true);

        final TestStorage testStorage = new TestPersistentStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);

        final AppMetrAsync appMetrAsync = new AppMetrAsync(appMetr);
        appMetrAsync.track(new Event("test1"));
        appMetrAsync.track(new Event("test2"));
        appMetrAsync.stop().join();

        verify(mockSender).send(eq(url), eq(token), any());

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void senderFail() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);

        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.setFailedUploadTimeout(Duration.ofMillis(10));
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();

        verify(mockSender, timeout(100).atLeast(2)).send(eq(url), eq(token), any());

        appMetr.hardStop();

        assertFalse(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void senderFailDoNotRetryUpload() throws IOException, InterruptedException {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);

        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchSender(mockSender);
        appMetr.setBatchStorage(testStorage);
        appMetr.setFailedUploadTimeout(Duration.ofMillis(1));
        appMetr.setRetryBatchUpload(false);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();
        Thread.sleep(2);
        verify(mockSender).send(eq(url), eq(token), any());

        appMetr.hardStop();

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    @Test
    void senderException() throws IOException, InterruptedException {
        final TestStorage testStorage = new TestStorage();

        appMetr.setBatchStorage(testStorage);
        appMetr.setFailedUploadTimeout(Duration.ofMillis(10));
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();
        Thread.sleep(10);

        assertThrows(RuntimeException.class, () -> appMetr.track(new Event("test2")));

        assertFalse(testStorage.getBathesQueue().isEmpty());

        assertTrue(RuntimeException.class.isAssignableFrom(appMetr.getLastUploadError().getClass()));
    }

    static void waitForever() throws InterruptedException {
        while (true) {
            Thread.sleep(100);
        }
    }

    static class TestStorage extends HeapStorage {
        List<List<Action>> storeCalls = new ArrayList<>();

        @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
            storeCalls.add(new ArrayList<>(actions));
            return super.store(actions, batchFactory);
        }

        Queue<BinaryBatch> getBathesQueue() {
            return batchesQueue;
        }
    }

    static class TestPersistentStorage extends TestStorage {
        @Override
        public boolean isPersistent() {
            return true;
        }
    }

    static class DiscardStorage extends HeapStorage {
        @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
            return false;
        }
    }

    static class NothingBatchSender implements BatchSender {
        static final NothingBatchSender instance = new NothingBatchSender();

        @Override public boolean send(String uri, String token, byte[] batch) {
            try {
                waitForever();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
