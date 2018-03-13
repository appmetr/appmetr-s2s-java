package com.appmetr.s2s.events;

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

    @Override public String toString() {
        return "Event{" +
                "event='" + event + '\'' +
                "} " + super.toString();
    }
}
