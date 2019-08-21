package com.appmetr.s2s.events;

public class AddToPropertiesValue extends Action {
    public static final String ACTION = "addToPropertiesValue";

    public AddToPropertiesValue() {
        super(ACTION);
    }

    @Override public String toString() {
        return "AddToPropertiesValue{} " + super.toString();
    }
}
