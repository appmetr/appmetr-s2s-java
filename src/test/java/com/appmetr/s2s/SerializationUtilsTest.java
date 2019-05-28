package com.appmetr.s2s;

import com.appmetr.s2s.events.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;

public class SerializationUtilsTest {

    @Test
    public void serializeEvent() throws Exception {
        Batch original = new Batch("s1", 1, singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);

        assertEquals(original, deserialized);
    }

    @Test()
    public void serializeLevel() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(new Level(2)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);

        assertEquals(original, deserialized);
    }

    @Test()
    public void serializePayment() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(
                new Payment("order1", "trans1", "proc1", "USD", "123", null, null, null, true)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }

    @Test
    public void serializeServerInstall() {
        Action install = ServerInstall.create()
                .setUserId("testUser")
                .setProperties(singletonMap("game", "wr"));
        Batch original = new Batch("s1", 1, singletonList(install));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);

        assertEquals(1, deserialized.getBatch().size());
        Action action = deserialized.getBatch().get(0);
        assertEquals("testUser", action.getUserId());
        assertTrue(action.getProperties().containsKey("game"));
        assertEquals("wr", action.getProperties().get("game"));

    }

    @Test(expected = RuntimeException.class)
    public void serializeWithoutTypeInfo() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s2", 9, singletonList(new Event("test"))), false);
        SerializationUtils.deserializeJsonGzip(bytes);
    }

    @Test()
    public void serializeUserTime() throws Exception {
        final Event event = new Event("test");
        event.setTimestamp(8);
        assertEquals(8, event.getTimestamp());

        Batch original = new Batch("s1", 2, singletonList(event));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, false);
        final JsonNode jsonNode = decompress(bytes);


        final ArrayNode batchNode = (ArrayNode) jsonNode.get("batch");
        assertEquals(8, batchNode.get(0).get("userTime").asLong());
    }

    @Test
    public void serializeWithoutOriginalTime() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, false);

        final JsonNode jsonNode = decompress(bytes);

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
