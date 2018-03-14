package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Batch {
    private String serverId;
    private int batchId;
    private List<Action> batch;

    private Batch() {
    }

    public Batch(String serverId, int batchId, List<Action> actionList) {
        this.serverId = serverId;
        this.batchId = batchId;
        batch = new ArrayList<>(actionList);
    }

    public String getServerId() {
        return serverId;
    }

    public int getBatchId() {
        return batchId;
    }

    public List<Action> getBatch() {
        return batch;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Batch batch1 = (Batch) o;
        return getBatchId() == batch1.getBatchId() &&
                Objects.equals(getServerId(), batch1.getServerId()) &&
                Objects.equals(getBatch(), batch1.getBatch());
    }

    @Override public int hashCode() {
        return Objects.hash(getServerId(), getBatchId(), getBatch());
    }

    @Override public String toString() {
        return "Batch{" +
                "serverId=" + getServerId() +
                ", batchId=" + getBatchId() +
                ", batch=" + getBatch() +
                '}';
    }
}
