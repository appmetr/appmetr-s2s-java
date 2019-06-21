package com.appmetr.s2s;

import com.appmetr.s2s.events.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;


class SerializationUtilsTest {

    @Test
    void serializeEvent_() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectMapper objectMapperTyped = objectMapper.copy();
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Action.class, new SerializationUtils.ActionJsonSerializer());
        module.addDeserializer(Action.class, new SerializationUtils.ActionJsonDeserializer());
        objectMapperTyped.registerModule(module);

        Batch original = new Batch("s1", 1, Collections.singletonList(new Event("test")));
        System.out.println(objectMapper.writeValueAsString(original));
        System.out.println(objectMapperTyped.writeValueAsString(original));
    }

    @Test
    void serializeEvent() throws Exception {
        Batch original = new Batch("s1", 1, singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);

        assertEquals(original, deserialized);
    }

    @Test()
    void serializeLevel() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(new Level(2)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);

        assertEquals(original, deserialized);
    }

    @Test()
    void serializePayment() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(
                new Payment("order1", "trans1", "proc1", "USD", "123", null, null, null, true)));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, true);
        Batch deserialized = SerializationUtils.deserializeJsonGzip(bytes);
        System.out.println(deserialized);
        assertEquals(original, deserialized);
    }

    @Test
    public void serializeServerInstall() {
        Action install = Events.serverInstall("testUser")
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

    @Test
    void serializeWithoutTypeInfo() throws Exception {
        byte[] bytes = SerializationUtils.serializeJsonGzip(new Batch("s2", 9, singletonList(new Event("test"))), false);
        assertThrows(RuntimeException.class, () -> SerializationUtils.deserializeJsonGzip(bytes));
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
    void serializeWithoutOriginalTime() throws Exception {
        Batch original = new Batch("s1", 2, singletonList(new Event("test")));
        byte[] bytes = SerializationUtils.serializeJsonGzip(original, false);

        final JsonNode jsonNode = decompress(bytes);

        final ArrayNode batchNode = (ArrayNode) jsonNode.get("batch");
        assertNull(batchNode.get(0).get("$userTime"));
    }

    static JsonNode decompress(byte[] compressedBody) throws IOException {
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
