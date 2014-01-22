package com.appmetr.s2s;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private int batchId;

    private ArrayList<Event> batch;

    public Batch(int batchId, List<Event> eventList) {
        this.batchId = batchId;
        batch = new ArrayList<Event>(eventList);
    }

    public int getBatchId() {
        return batchId;
    }

    public ArrayList<Event> getBatch() {
        return batch;
    }

    @Override public String toString() {
        return "Batch{" +
                "#events=" + batch.size() +
                ", batchId=" + batchId +
                '}';
    }
}
