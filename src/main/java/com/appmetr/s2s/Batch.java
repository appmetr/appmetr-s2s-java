package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private String serverId;
    private int batchId;
    private ArrayList<Action> batch;

    public Batch(String serverId, int batchId, List<Action> actionList) {
        this.serverId = serverId;
        this.batchId = batchId;
        batch = new ArrayList<Action>(actionList);
    }

    public String getServerId() {
        return serverId;
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
                ", serverId=" + serverId +
                ", batchId=" + batchId +
                '}';
    }
}
