package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.util.Collection;

public class HeapStorage extends AbstractHeapStorage {

    /**
     * Store operation is always successful but can block until space become available
     * @return always true
     */
    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
        return super.store(actions, batchFactory);
    }

    @Override protected boolean isCapacityExceeded(BinaryBatch binaryBatch) throws InterruptedException {
        while (true) {
            final boolean capacityExceeded = super.isCapacityExceeded(binaryBatch);
            if (!capacityExceeded) {
                return false;
            }
            wait();
        }
    }

    @Override public synchronized void remove() {
        super.remove();
        notifyAll();
    }
}
