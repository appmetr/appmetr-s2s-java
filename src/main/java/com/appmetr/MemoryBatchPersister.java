package com.appmetr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;

public class MemoryBatchPersister implements BatchPersister {
    protected static final Logger logger = LoggerFactory.getLogger("MemoryBatchPersister");

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
                logger.warn("trying to delete not last branch");
            }else{
                batchStack.pollLast();
            }
        }
    }
}
