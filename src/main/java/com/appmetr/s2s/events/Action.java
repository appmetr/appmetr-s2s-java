package com.appmetr.s2s.events;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class Action {
    private String action;
    private long timestamp = new Date().getTime();
    private Map<String, Object> properties = new HashMap<>();
    private String userId;
    private long userTime;

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
        return userTime == 0 ? timestamp : userTime;
    }

    public Action setTimestamp(long timestamp) {
        this.userTime = timestamp;
        return this;
    }

    public int calcApproximateSize() {
        int size = 40 + (40 * properties.size()); //40 - Map size and 40 - each entry overhead

        size += getStringLength(action);
        size += getStringLength(String.valueOf(timestamp));
        size += getStringLength(userId);

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            size += getStringLength(entry.getKey());
            size += getStringLength(entry.getValue() != null ? entry.getValue().toString() : null);   //toString because sending this object via json
        }

        return 8 + size + 8; //8 - object header
    }

    protected int getStringLength(String str) {
        return str == null ? 0 : str.length() * Character.BYTES + 24 + 16;    //24 - String object size, 16 - char[]
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action action1 = (Action) o;
        return getTimestamp() == action1.getTimestamp() &&
                Objects.equals(getAction(), action1.getAction()) &&
                Objects.equals(getProperties(), action1.getProperties()) &&
                Objects.equals(getUserId(), action1.getUserId());
    }

    @Override public int hashCode() {
        return Objects.hash(getAction(), getTimestamp(), getProperties(), getUserId());
    }

    @Override public String toString() {
        return "Action{" +
                "action='" + getAction() + '\'' +
                ", timestamp=" + Instant.ofEpochMilli(timestamp) +
                ", userTime=" + Instant.ofEpochMilli(getTimestamp()) +
                ", properties=" + getProperties() +
                ", userId='" + getUserId() + '\'' +
                '}';
    }
}
