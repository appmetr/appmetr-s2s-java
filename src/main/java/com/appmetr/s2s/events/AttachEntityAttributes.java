package com.appmetr.s2s.events;

public class AttachEntityAttributes extends Action {
    public static final String ACTION = "attachEntityAttributes";

    private String entityName;
    private String entityValue;

    public AttachEntityAttributes(String entityName, String entityValue) {
        super(ACTION);
        this.entityName = entityName;
        this.entityValue = entityValue;
    }

    @Override public String toString() {
        return "AttachEntityAttributes{} " + super.toString();
    }
}
