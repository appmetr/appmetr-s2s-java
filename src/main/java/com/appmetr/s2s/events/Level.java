package com.appmetr.s2s.events;

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

    @Override public String toString() {
        return "Level{" +
                "level=" + level +
                "} " + super.toString();
    }
}
