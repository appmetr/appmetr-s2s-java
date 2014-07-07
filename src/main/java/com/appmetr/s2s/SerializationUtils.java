package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.events.ActionAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

}
