package com.appmetr.s2s;

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

    public static byte[] serializeJsonGzip(Batch batch) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream gzos = null;

        try {
            gzos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION, true));
            gzos.write(gson.toJsonTree(batch).toString().getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (gzos != null) try { gzos.close(); } catch (IOException ioException) {
                logger.info("Can't close gzip output stream", ioException);
            } ;
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

            return gson.fromJson(inflatedByteStream.toString(), Batch.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
