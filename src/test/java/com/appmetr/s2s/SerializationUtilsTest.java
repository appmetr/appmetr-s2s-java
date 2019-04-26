package com.appmetr.s2s;

import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.events.Level;
import com.appmetr.s2s.events.Payment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    public void serializeUserTime() throws Exception {
        final Event event = new Event("test");
        event.setTimestamp(8);
        assertEquals(8, event.getTimestamp());

        Batch original = new Batch("s1", 2, Collections.singletonList(event));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, false);
        final JsonNode jsonNode = decompress(bytes);
        System.out.println(jsonNode);

        final ArrayNode batchNode = (ArrayNode) jsonNode.get("batch");
        assertEquals(8, batchNode.get(0).get("$userTime").asLong());
    }

    @Test
    public void serializeWithoutOriginalTime() throws Exception {
        Batch original = new Batch("s1", 2, Collections.singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, false);

        final JsonNode jsonNode = decompress(bytes);
        System.out.println(jsonNode);

        final ArrayNode batchNode = (ArrayNode) jsonNode.get("batch");
        assertNull(batchNode.get(0).get("$userTime"));
    }

    public static JsonNode decompress(byte[] compressedBody) throws IOException {
        ByteArrayOutputStream inflatedByteStream = new ByteArrayOutputStream();
        Inflater inflater = new Inflater(true);
        try (InflaterOutputStream inflateStream = new InflaterOutputStream(inflatedByteStream, inflater)) {
            inflateStream.write(compressedBody);
            inflateStream.finish();
            return SerializationUtils.objectMapper.readTree(inflatedByteStream.toString(StandardCharsets.UTF_8.name()));
        } finally {
            inflater.end();
        }
    }

}
