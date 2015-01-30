package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.util.ArrayDeque;
import java.util.List;

public class MemoryBatchPersister implements BatchPersister {

    private final ArrayDeque<Batch> batchQueue = new ArrayDeque<Batch>() {};
    private int batchId = 0;

    @Override public Batch getNext() {
        synchronized (batchQueue) {
            if (batchQueue.size() == 0) return null;
            return batchQueue.peekFirst();
        }
    }

    @Override public void persist(List<Action> actionList) {
        synchronized (batchQueue) {
            Batch batch = new Batch(batchId++, actionList);
            batchQueue.push(batch);
        }
    }

    @Override public void remove() {
        synchronized (batchQueue) {
            batchQueue.pollFirst();
        }
    }
}
