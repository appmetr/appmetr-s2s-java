package com.appmetr.s2s.events;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class Action {
    private String action;
    private long timestamp = new Date().getTime();
    private Map<String, Object> properties = new HashMap<String, Object>();
    private String userId;

    public Action(String action) {
        this.action = action;
    }

    public String getAction() {
        return action;
    }

    public String getUserId() {
        return userId;
    }

    public Action setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Action setProperties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Action setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public int calcApproximateSize() {
        int size = 40 + (40 * properties.size()); //40 - Map size and 40 - each entry overhead

        size += getStringLength(action);
        size += getStringLength(String.valueOf(timestamp));
        size += getStringLength(userId);

        if (userId != null) {
            size += userId.length() * 2 + 24 + 16;
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            size += getStringLength(entry.getKey());
            size += getStringLength(entry.getValue().toString());   //toString because sending this object via json
        }

        return 8 + size + 8; //8 - object header
    }

    protected int getStringLength(String str) {
        return str == null ? 0 : str.length() * 2 + 24 + 16;    //24 - String object size, 16 - char[]
    }
}
