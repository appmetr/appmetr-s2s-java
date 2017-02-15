package com.appmetr.s2s.persister;

import com.appmetr.s2s.AppMetr;
import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MemoryBatchPersister implements BatchPersister {

    private final Queue<Batch> batchQueue = new ArrayDeque<>();
    private int batchId = 0;

    @Override public Batch getNext() {
        synchronized (batchQueue) {
            if (batchQueue.size() == 0) return null;
            return batchQueue.peek();
        }
    }

    @Override public void persist(List<Action> actionList) {
        synchronized (batchQueue) {
            Batch batch = new Batch(AppMetr.SERVER_ID, batchId++, actionList);
            batchQueue.add(batch);
        }
    }

    @Override public void remove() {
        synchronized (batchQueue) {
            batchQueue.poll();
        }
    }
}
