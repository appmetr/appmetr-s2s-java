package com.appmetr;

import com.google.gson.JsonObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class SerializationUtils {
    private static Logger logger = Logger.getLogger("SerializationUtils");

    public static byte[] serializeGzip(Batch batch){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream gzos = null;

        try {
            JsonObject json = batch.toJson();

            gzos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION, true));
            gzos.write(json.toString().getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            if (gzos != null) try { gzos.close(); } catch (IOException ioException) {
                logger.info("Cant close gzip output stream: " + ioException);
            };
        }

        return baos.toByteArray();
    }
}
