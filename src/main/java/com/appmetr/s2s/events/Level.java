package com.appmetr.s2s.events;

import java.util.Objects;

public class Level extends Action {
    private static final String ACTION = "trackLevel";

    private int level;

    //Only for Jackson deserialization
    private Level() {
        super(ACTION);
    }

    public Level(int level) {
        this();
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + 4;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Level level1 = (Level) o;
        return getLevel() == level1.getLevel();
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getLevel());
    }

    @Override public String toString() {
        return "Level{" +
                "level=" + getLevel() +
                "} " + super.toString();
    }
}
