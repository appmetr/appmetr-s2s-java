package com.appmetr.s2s;

import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.persister.FileBatchPersister;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class AppMetrTest {

    private static String token = "";
    private static String url = "";
    private static String filePersisterPath = "";

    @Test
    public void testPersister() {
        AppMetr appMetr = new AppMetr(token, url, new FileBatchPersister(filePersisterPath));
        for (int i = 0; i < 10001; i++) {
            HashMap<String, Object> properties = new HashMap<String, Object>();

            for (int j = 0; j < 25; j++) {
                properties.put(String.valueOf(j), getRandomObject());
            }
            appMetr.track(new Event("event#" + i % 100).setProperties(properties));
        }
        appMetr.stop();
    }

    @Test
    public void pushSomeEvents() {
        AppMetr appMetr = new AppMetr(token, url);
        for (int i = 0; i < 500; i++) {
            HashMap<String, Object> properties = new HashMap<String, Object>();

            for (int j = 0; j < 5; j++) {
                properties.put(String.valueOf(j), getRandomObject());
            }
            appMetr.track(new Event("event#" + i).setProperties(properties));
        }
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
}
