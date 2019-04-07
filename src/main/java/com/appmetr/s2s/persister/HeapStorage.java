package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;

public class HeapStorage extends NonBlockingHeapStorage {

    @Override public synchronized void remove() {
        super.remove();
        notifyAll();
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
}
