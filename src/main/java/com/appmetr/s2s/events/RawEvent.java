package com.appmetr.s2s.events;

import java.util.Objects;

/**
 * This event is saved as is.
 * Can contain field names which start with $
 */
public class RawEvent extends Action {
    public static final String ACTION = "trackRawEvent";

    private String event;

    //Only for Jackson deserialization
    private RawEvent() {
        super(ACTION);
    }

    public RawEvent(String event) {
        this();
        this.event = event;
    }

    public String getEvent() {
        return event;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + getStringLength(event);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RawEvent event1 = (RawEvent) o;
        return Objects.equals(getEvent(), event1.getEvent());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getEvent());
    }

    @Override public String toString() {
        return "Event{" +
                "event='" + getEvent() + '\'' +
                "} " + super.toString();
    }
}
