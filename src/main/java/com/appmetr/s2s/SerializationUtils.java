package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

public class SerializationUtils {
    private static Logger logger = LoggerFactory.getLogger(SerializationUtils.class);

    private static final String CLASS_NAME = "cls";
    private static final String INSTANCE = "inst";
    public static final ObjectMapper objectMapper;
    public static final ObjectMapper objectMapperTyped;

    static {
        objectMapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT)
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapperTyped = objectMapper.copy();
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Action.class, new ActionJsonSerializer());
        module.addDeserializer(Action.class, new ActionJsonDeserializer());
        objectMapperTyped.registerModule(module);
    }

    public static byte[] serializeJsonGzip(Batch batch, boolean withType) {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DeflaterOutputStream gzos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION, true))) {

            if (withType) {
                objectMapperTyped.writeValue(gzos, batch);
            } else {
                objectMapper.writeValue(gzos, batch);
            }

            gzos.finish();

            return baos.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Batch deserializeJsonGzip(byte[] deflatedBytes) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             InflaterOutputStream uzos = new InflaterOutputStream(baos, new Inflater(true))) {

            uzos.write(deflatedBytes);

            uzos.finish();

            return objectMapperTyped.readValue(baos.toByteArray(), Batch.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class ActionJsonSerializer extends StdSerializer<Action> {

        ActionJsonSerializer() {
            super(Action.class);
        }

        @Override public void serialize(Action action, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeStringField(CLASS_NAME, action.getClass().getSimpleName());
            gen.writeFieldName(INSTANCE);
            gen.writeRawValue(objectMapper.writeValueAsString(action));
            gen.writeEndObject();
        }
    }

    private static class ActionJsonDeserializer extends StdDeserializer<Action> {

        ActionJsonDeserializer() {
            super(Action.class);
        }

        @Override public Action deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final ObjectCodec oc = p.getCodec();
            final JsonNode node = oc.readTree(p);

            if (!node.has(CLASS_NAME) || !node.has(INSTANCE)) {
                throw new IllegalArgumentException("Can't deserialize object without " + CLASS_NAME + " or " + INSTANCE + " property: " + node);
            }

            final String className = node.get(CLASS_NAME).textValue();
            final JsonNode action = node.get(INSTANCE);
            try {
                @SuppressWarnings("unchecked")
                final Class<? extends Action> aClass = (Class<? extends Action>) Class.forName(Action.class.getPackage().getName() + "." + className);

                return objectMapper.treeToValue(action, aClass);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Couldn't find class " + className);
            }
        }
    }
}
