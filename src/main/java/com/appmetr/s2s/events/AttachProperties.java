package com.appmetr.s2s.events;

public class AttachProperties extends Action {
    public static final String ACTION = "attachProperties";

    public AttachProperties() {
        super(ACTION);
    }

    @Override public String toString() {
        return "AttachProperties{} " + super.toString();
    }
}
