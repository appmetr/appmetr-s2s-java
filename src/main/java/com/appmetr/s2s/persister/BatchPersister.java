package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.Event;

import java.util.List;

public interface BatchPersister {

    /**
     * Get the oldest batch from storage, but dont remove it.
     */
    public Batch getNext();

    /**
     * Persist list of events as Batch.
     *
     * @param eventList list of events.
     */
    public void persist(List<Event> eventList);

    /**
     * Remove oldest batch from storage.
     */
    public void remove();
}
