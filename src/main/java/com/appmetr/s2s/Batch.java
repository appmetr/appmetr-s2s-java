package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private int batchId;

    private ArrayList<Action> batch;

    public Batch(int batchId, List<Action> actionList) {
        this.batchId = batchId;
        batch = new ArrayList<Action>(actionList);
    }

    public int getBatchId() {
        return batchId;
    }

    public ArrayList<Action> getBatch() {
        return batch;
    }

    @Override public String toString() {
        return "Batch{" +
                "#events=" + batch.size() +
                ", batchId=" + batchId +
                '}';
    }
}
