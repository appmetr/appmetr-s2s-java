package com.appmetr.s2s.events;

public class Event extends Action {
    private static final String ACTION = "trackEvent";

    private String event;

    public Event(String event) {
        super(ACTION);

        this.event = event;
    }

    public String getEvent() {
        return event;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + getStringLength(event);
    }
}
