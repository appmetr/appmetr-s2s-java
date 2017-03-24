package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.util.List;

public interface BatchPersister {

    /**
     * Get the oldest batch from storage, but dont remove it.
     */
    public Batch getNext();

    /**
     * Persist list of events as Batch.
     *
     * @param actionList list of events.
     */
    public void persist(List<Action> actionList);

    /**
     * Remove oldest batch from storage.
     */
    public void remove();

    public void setServerId(String serverId);
}
