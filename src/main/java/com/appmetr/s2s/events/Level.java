package com.appmetr.s2s.events;

public class Level extends Action {
    private static final String ACTION = "trackLevel";

    private int level;

    public Level(int level) {
        super(ACTION);

        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + 4;
    }
}
