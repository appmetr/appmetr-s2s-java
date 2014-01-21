package com.appmetr;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private ArrayList<Event> batch;

    private int batchId;

    public int getBatchId() {
        return batchId;
    }

    public Batch(int batchId, List<Event> eventList) {
        this.batchId = batchId;
        batch = new ArrayList<Event>(eventList);
    }

    public JsonObject toJson() {
        JsonObject batch = new JsonObject();
        batch.addProperty("batchId", batchId);

        batch.add("batch", new GsonBuilder().create().toJsonTree(this.batch));

        return batch;
    }

    @Override public String toString() {
        return "Batch{" +
                "#events=" + batch.size() +
                ", batchId=" + batchId +
                '}';
    }
}
