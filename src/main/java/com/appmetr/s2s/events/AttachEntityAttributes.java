package com.appmetr.s2s.events;

import java.util.Objects;

public class AttachEntityAttributes extends Action {
    public static final String ACTION = "attachEntityAttributes";

    private String entityName;
    private String entityValue;

    public AttachEntityAttributes(String entityName, String entityValue) {
        super(ACTION);
        this.entityName = entityName;
        this.entityValue = entityValue;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttachEntityAttributes)) return false;
        if (!super.equals(o)) return false;
        AttachEntityAttributes that = (AttachEntityAttributes) o;
        return Objects.equals(entityName, that.entityName) &&
                Objects.equals(entityValue, that.entityValue);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), entityName, entityValue);
    }

    @Override public String toString() {
        return "AttachEntityAttributes{} " + super.toString();
    }
}
