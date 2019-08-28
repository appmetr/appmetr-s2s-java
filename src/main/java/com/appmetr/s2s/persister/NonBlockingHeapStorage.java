package com.appmetr.s2s.persister;

import com.appmetr.s2s.events.Action;

import java.util.Collection;

public class NonBlockingHeapStorage extends AbstractHeapStorage {

    protected NonBlockingHeapStorage() {
    }

    public NonBlockingHeapStorage(long maxBytes) {
        super(maxBytes);
    }

    /**
     * Never blocks
     */
    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) {
        try {
            return super.store(actions, batchFactory);
        } catch (InterruptedException e) {
            throw new IllegalStateException("It is imposable to be interrupted here");
        }
    }
}
