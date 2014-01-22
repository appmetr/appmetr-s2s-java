package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.Event;

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

    @Override public void persist(List<Event> eventList) {
        synchronized (batchStack) {
            Batch batch = new Batch(batchId++, eventList);
            batchStack.push(batch);
        }
    }

    @Override public void remove() {
        synchronized (batchStack) {
            batchStack.pollLast();
        }
    }
}
