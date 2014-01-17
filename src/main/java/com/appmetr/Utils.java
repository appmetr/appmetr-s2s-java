package com.appmetr;

public class Utils {
    public static String getEncodedString(int batchId, Event event){
        return batchId + "\r\n" + event.toString();
    }
}
