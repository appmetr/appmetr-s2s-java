package com.appmetr.s2s;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Event {

    private String event;
    private long timestamp;
    private Map<String, Object> properties;

    public Event(String event){
        this(event, new Date().getTime(), new HashMap<String, Object>());
    }

    public Event(String event, Map<String, Object> properties){
        this(event, new Date().getTime(), properties);
    }

    public Event(String event, long timestamp, Map<String, Object> properties) {
        this.event = event;
        this.timestamp = timestamp;
        this.properties = properties;
    }

    public String getEventName() {
        return event;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int calcApproximateSize() {
        int propertiesSize = 40 + (40 * properties.size()); //40 - Map size and 40 - each entry overhead
        propertiesSize += String.valueOf(timestamp).length() * 2 + 24 + 16;

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            propertiesSize += entry.getKey().length() * 2 + 24 + 16;    //24 - String object size, 16 - char[]
            propertiesSize += entry.getValue().toString().length() * 2 + 24 + 16; //toString because sending this object via json
        }

        return 8 + propertiesSize + 8; //8 - object header, 8 - double
    }
}
