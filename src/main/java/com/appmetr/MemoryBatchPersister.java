package com.appmetr;

import java.util.ArrayDeque;
import java.util.List;

public class MemoryBatchPersister implements BatchPersister {
    private ArrayDeque<Batch> batchStack = new ArrayDeque<Batch>() {};
    private int batchId = 0;

    @Override public Batch getNextBatch() {
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

    @Override public void deleteLastBatch(int batchId) {
        synchronized (batchStack) {
            Batch batch = batchStack.peekLast();

            if(batch == null || batch.getBatchId() != batchId){
                //TODO: what to do here???
            }else{
                batchStack.pollLast();
            }
        }
    }
}
