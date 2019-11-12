package com.appmetr.s2s.events;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Action {
    private static final int BITS_PER_TIMESTAMP_MS = 42; //max value is 2106 year
    private static final int BITS_PER_COUNTER = 64 - BITS_PER_TIMESTAMP_MS; //22 bits, max value is 4M
    private static final AtomicLong timeKeyCounter = new AtomicLong();

    private String action;
    private long timestamp = System.currentTimeMillis();
    private Map<String, Object> properties = new HashMap<>();
    private String userId;
    private long userTime;
    private long timeKey;
    private long userTimeKey;

    public Action(String action) {
        this.action = action;
        this.timeKey = createTimeKey(timestamp);
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

    public Action setTimeKey(long timeKey) {
        this.userTimeKey = timeKey;
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

    public static long createTimeKey(long timestampMs) {
        return (timestampMs << BITS_PER_COUNTER) | timeKeyCounter.getAndIncrement();
    }
}
