package com.appmetr;

import java.util.List;

public interface BatchPersister {
    public Batch getNextBatch();
    public void persist(List<Event> eventList);
    public void deleteLastBatch(int batchId);
}
