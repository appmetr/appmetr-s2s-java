package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;

/**
 * Method store() is always successful and always returns true but can block until space become available
 */
public class HeapStorage extends AbstractHeapStorage {

    protected HeapStorage() {
    }

    public HeapStorage(long maxBytes) {
        super(maxBytes);
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
