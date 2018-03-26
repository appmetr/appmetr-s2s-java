package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MemoryBatchPersister implements BatchPersister {

    private final Queue<Batch> batchQueue = new ArrayDeque<>();
    private int batchId = 0;
    private String serverId;

    @Override public Batch getNext() {
        synchronized (batchQueue) {
            return batchQueue.peek();
        }
    }

    @Override public void persist(List<Action> actionList) {
        synchronized (batchQueue) {
            batchQueue.add(new Batch(serverId, batchId++, actionList));
        }
    }

    @Override public void remove() {
        synchronized (batchQueue) {
            batchQueue.poll();
        }
    }

    @Override public void setServerId(String serverId) {
        this.serverId = serverId;
    }
}
