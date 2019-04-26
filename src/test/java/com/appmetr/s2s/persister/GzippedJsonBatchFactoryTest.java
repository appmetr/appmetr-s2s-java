package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Level;
import com.appmetr.s2s.events.Payment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GzippedJsonBatchFactoryTest {

    @Test
    void createBatch() throws Exception {
        final BinaryBatch batch = GzippedJsonBatchFactory.instance.createBatch(Arrays.asList(
                new Level(2).setProperties(Collections.singletonMap("a", 5)),
                new Payment("order1", "trans1", "proc1", "USD", "123")), 1, "s1");

        final JsonNode jsonNode = decompress(batch.getBytes());
        assertEquals(1, jsonNode.get("batchId").asLong());
        assertEquals("s1", jsonNode.get("serverId").asText());

        final ArrayNode events = (ArrayNode) jsonNode.get("batch");
        assertTrue(events.isArray());
        assertEquals(2, events.size());

        assertEquals("trackLevel", events.get(0).get("action").asText());
        assertEquals("trackPayment", events.get(1).get("action").asText());

        assertEquals(5, events.get(0).get("properties").get("a").asInt());
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
