package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

public class SerializationUtilsGson {
    private static Logger logger = LoggerFactory.getLogger(SerializationUtils.class);

    private static Gson gson = new GsonBuilder().create();
    private static Gson gsonTyped = new GsonBuilder().registerTypeHierarchyAdapter(Action.class, new ActionAdapter()).create();

    public static byte[] serializeJsonGzip(Batch batch, boolean withType) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream gzos = null;

        try {
            gzos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION, true));
            gzos.write(withType ? gsonTyped.toJson(batch).getBytes() : gson.toJson(batch).getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (gzos != null) try { gzos.close(); } catch (IOException ioException) {
                logger.info("Can't close gzip output stream", ioException);
            }
        }

        return baos.toByteArray();
    }

    public static Batch deserializeJsonGzip(byte[] deflatedBytes) {
        try {
            ByteArrayOutputStream inflatedByteStream = new ByteArrayOutputStream();

            InflaterOutputStream inflateStream = new InflaterOutputStream(inflatedByteStream, new Inflater(true));
            inflateStream.write(deflatedBytes);
            inflateStream.flush();
            inflateStream.finish();
            inflateStream.close();

            return gsonTyped.fromJson(inflatedByteStream.toString(), Batch.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ActionAdapter implements JsonSerializer<Action>, JsonDeserializer<Action> {
        private final String CLASS_NAME = "cls";
        private final String INSTANCE = "inst";

        private final Gson gson = new GsonBuilder().create();

        @Override public JsonElement serialize(Action action, Type type, JsonSerializationContext context) {
            JsonObject result = new JsonObject();
            result.addProperty(CLASS_NAME, action.getClass().getSimpleName());
            result.add(INSTANCE, gson.toJsonTree(action));

            return result;
        }

        @Override public Action deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonObject obj = jsonElement.getAsJsonObject();
            if (!obj.has(CLASS_NAME)) {
                return context.deserialize(jsonElement, type);
            } else {
                try {
                    Class cls = Class.forName("com.appmetr.s2s.events." + obj.get(CLASS_NAME).getAsString());

                    return gson.<Action>fromJson(obj.get(INSTANCE), cls);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Couldn't find class " + obj.get(CLASS_NAME));
                }
            }
        }
    }
}
