package com.appmetr.s2s;

import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.events.Level;
import com.appmetr.s2s.events.Payment;
import org.junit.Test;

import java.util.Collections;

public class SerializationUtilsTest {

    @Test
    public void serializeEvent() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s1", 1, Collections.singletonList(new Event("test"))), true);
        Batch batch = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(batch);
    }

    @Test()
    public void serializeLevel() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s1", 2, Collections.singletonList(new Level(2))), true);
        Batch batch = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(batch);
    }

    @Test()
    public void serializePayment() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s1", 2, Collections.singletonList(
                new Payment("order1", "trans1", "proc1", "USD", "123"))), true);
        Batch batch = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(batch);
    }

    @Test(expected = RuntimeException.class)
    public void serializeWithoutTypeInfo() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s2", 9, Collections.singletonList(new Event("test"))), false);
        Batch batch = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(batch);
    }
}
