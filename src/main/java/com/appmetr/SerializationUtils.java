package com.appmetr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

public class SerializationUtils {
    private static Logger logger = Logger.getLogger("SerializationUtils");

    public static byte[] serializeGzip(String str){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzos = null;

        try {
            gzos = new GZIPOutputStream(baos);
            gzos.write(str.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            if (gzos != null) try { gzos.close(); } catch (IOException ignore) {
                logger.info("PAAAANIC");
            };
        }

        return baos.toByteArray();
    }
}
