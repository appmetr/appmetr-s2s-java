package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

public class AbstractHeapStorage implements BatchStorage {

    protected Queue<BinaryBatch> batchQueue = new ArrayDeque<>();
    protected Clock clock = Clock.systemUTC();
    protected long maxBytes = -1;

    protected long previousBatchId;
    protected long occupiedBytes;

    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
        long batchId = clock.millis();
        if (batchId <= previousBatchId) {
            batchId = previousBatchId + 1;
        }

        final BinaryBatch binaryBatch = batchFactory.createBatch(actions, batchId);
        if (isCapacityExceeded(binaryBatch)) {
            return false;
        }

        occupiedBytes += binaryBatch.getBytes().length;
        previousBatchId = batchId;
        batchQueue.add(binaryBatch);

        notify();

        return true;
    }

    @Override public synchronized BinaryBatch peek() throws InterruptedException {
        while (true) {
            final BinaryBatch binaryBatch = batchQueue.peek();
            if (binaryBatch != null) {
                return binaryBatch;
            }
            wait();
        }
    }

    @Override public synchronized void remove() {
        final BinaryBatch binaryBatch = batchQueue.poll();
        if (binaryBatch != null) {
            occupiedBytes -= binaryBatch.getBytes().length;
        }
    }

    protected boolean isCapacityExceeded(BinaryBatch binaryBatch) throws InterruptedException {
        return occupiedBytes + binaryBatch.getBytes().length > maxBytes;
    }
}
