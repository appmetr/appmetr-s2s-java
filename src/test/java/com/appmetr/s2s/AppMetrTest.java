package com.appmetr.s2s;


import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.persister.BatchFactory;
import com.appmetr.s2s.persister.GzippedJsonBatchFactoryTest;
import com.appmetr.s2s.persister.HeapStorage;
import com.appmetr.s2s.sender.BatchSender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class AppMetrTest {

    private static String token = "testToken";
    private static String url = "testUrl";

    @Test
    void sendManually() throws Exception {
        final BatchSender mockSender = Mockito.mock(BatchSender.class);

        final AppMetr appMetr = new AppMetr(token, url);
        appMetr.setServerId("s1");
        appMetr.setBatchSender(mockSender);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));

        appMetr.flush();
        Thread.sleep(1);

        ArgumentCaptor<byte[]> batch1 = ArgumentCaptor.forClass(byte[].class);
        verify(mockSender).send(eq(url), eq(token), batch1.capture());
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

        final AppMetr appMetr = new AppMetr(token, url);
        appMetr.setServerId("s1");
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

        final AppMetr appMetr = new AppMetr(token, url);
        appMetr.setServerId("s1");
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

    private static Object getRandomObject() {
        Random random = new Random();
        int switcher = random.nextInt(5);
        switch (switcher) {
            case 0: return "2012-08-21";
            case 1: return random.nextBoolean();
            case 2:
                ArrayList<Integer> randomList = new ArrayList<Integer>(5);
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
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw e;
            }
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

    static class NothingBatchSender implements BatchSender {
        static final NothingBatchSender instance = new NothingBatchSender();

        @Override public boolean send(String uri, String token, byte[] batches) {
            try {
                waitForever();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
