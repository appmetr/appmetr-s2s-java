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

        appMetr.stop();
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

        appMetr.stop();

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

        appMetr.stop();

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

        appMetr.setClock(Clock.fixed(Instant.ofEpochSecond(2), ZoneOffset.UTC));

        assertTrue(appMetr.track(event2));

        assertEquals(1, testStorage.storeCalls.size());
        assertEquals(Collections.singletonList(event1), testStorage.storeCalls.get(0));

        appMetr.stop();

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

        appMetr.stop();
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
        
        appMetr.stop();

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

        verify(mockSender, timeout(100)).send(eq(url), eq(token), any());

        appMetr.stop();

        assertTrue(testStorage.getBathesQueue().isEmpty());
    }

    private static Object getRandomObject() {
        Random random = new Random();
        int switcher = random.nextInt(5);
        switch (switcher) {
            case 0: return "2012-08-21";
            case 1: return random.nextBoolean();
            case 2:
                ArrayList<Integer> randomList = new ArrayList<>(5);
                for (int i = 0; i < 5; i++) {
                    randomList.add(random.nextInt());
                }
                return randomList;
            case 3: return random.nextDouble();
            case 4: return generateString(random, "abcdefght", 8);
            default: return "DEFAULT";
        }
    }

    private static String generateString(Random rng, String characters, int length) {
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
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
