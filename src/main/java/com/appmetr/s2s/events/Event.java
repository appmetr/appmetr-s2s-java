package com.appmetr.s2s.events;

import java.util.Objects;

public class Event extends Action {
    private static final String ACTION = "trackEvent";

    private String event;

    //Only for Jackson deserialization
    private Event() {
        super(ACTION);
    }

    public Event(String event) {
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
        Event event1 = (Event) o;
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
