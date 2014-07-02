package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.util.ArrayDeque;
import java.util.List;

public class MemoryBatchPersister implements BatchPersister {

    private ArrayDeque<Batch> batchStack = new ArrayDeque<Batch>() {};
    private int batchId = 0;

    @Override public Batch getNext() {
        synchronized (batchStack) {
            if (batchStack.size() == 0) return null;
            return batchStack.peekLast();
        }
    }

    @Override public void persist(List<Action> actionList) {
        synchronized (batchStack) {
            Batch batch = new Batch(batchId++, actionList);
            batchStack.push(batch);
        }
    }

    @Override public void remove() {
        synchronized (batchStack) {
            batchStack.pollLast();
        }
    }
}
