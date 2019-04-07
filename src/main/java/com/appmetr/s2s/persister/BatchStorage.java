package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.io.IOException;
import java.util.Collection;

public interface BatchStorage {

    /**
     * Inserts the specified element into this storage if it is possible to do
     * so immediately without violating capacity restrictions, returning
     * {@code true} upon success and {@code false} if no space is currently
     * available.
     * Can blocks until space become available.
     *
     * @return {@code true} if the element was added to this storage, else
     *         {@code false}
     * @throws InterruptedException if interrupted while waiting
     */
    boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException, IOException;

    /**
     * Retrieves, but does not remove, the head of this storage, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this storage
     * @throws InterruptedException if interrupted while waiting
     */
    BinaryBatch peek() throws InterruptedException, IOException;

    /**
     * Removes the head of this storage.
     */
    void remove();
}
