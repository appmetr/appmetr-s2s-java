package com.appmetr.s2s;


import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.persister.BatchStorage;
import com.appmetr.s2s.sender.BatchSender;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppMetrTest {

    private static String token = "";
    private static String url = "";

    @Test
    void storeByActionsNumber() throws Exception {
        final BatchStorage mockStorage = Mockito.mock(BatchStorage.class);
        Mockito.when(mockStorage.store(any(), any())).thenReturn(true);
        when(mockStorage.peek()).thenAnswer(invocation -> {
            waitForever();
            return null;
        });

        final AppMetr appMetr = new AppMetr(token, url);
        appMetr.setBatchSender(NothingBatchSender.instance);
        appMetr.setBatchStorage(mockStorage);
        appMetr.setMaxBatchActions(1);
        appMetr.start();

        assertTrue(appMetr.track(new Event("test1")));
        assertTrue(appMetr.track(new Event("test2")));

        appMetr.stop();

        verify(mockStorage).store(eq(Collections.singleton(new Event("test1"))), any());
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
