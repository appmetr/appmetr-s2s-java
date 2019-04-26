package com.appmetr.s2s;

import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.events.Level;
import com.appmetr.s2s.events.Payment;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SerializationUtilsTest {

    @Test
    public void serializeEvent() throws Exception {
        Batch original = new Batch("s1", 1, Collections.singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }

    @Test()
    public void serializeLevel() throws Exception {
        Batch original = new Batch("s1", 2, Collections.singletonList(new Level(2)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }

    @Test()
    public void serializePayment() throws Exception {
        Batch original = new Batch("s1", 2, Collections.singletonList(
                new Payment("order1", "trans1", "proc1", "USD", "123", null, null, null, true)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }

    @Test(expected = RuntimeException.class)
    public void serializeWithoutTypeInfo() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s2", 9, Collections.singletonList(new Event("test"))), false);
        SerializationUtils.deserializeJsonGzip(bytes);
    }

    @Test()
    public void serializeOriginalTime() throws Exception {
        final Event event = new Event("test");
        event.setTimestamp(8);
        assertEquals(8, event.getTimestamp());

        Batch original = new Batch("s1", 2, Collections.singletonList(event));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }
}
