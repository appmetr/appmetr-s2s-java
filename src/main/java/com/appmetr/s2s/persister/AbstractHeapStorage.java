package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

public class AbstractHeapStorage implements BatchStorage {

    protected Queue<BinaryBatch> batchesQueue = new ArrayDeque<>();
    protected Clock clock = Clock.systemUTC();
    protected long maxBytes;

    protected long previousBatchId;
    protected long occupiedBytes;

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
        long batchId = clock.millis();
        if (batchId <= previousBatchId) {
            batchId = previousBatchId + 1;
        }

        return store(batchFactory.createBatch(actions, batchId));
    }

    protected synchronized boolean store(BinaryBatch binaryBatch) throws InterruptedException {
        if (isCapacityExceeded(binaryBatch)) {
            return false;
        }

        occupiedBytes += binaryBatch.getBytes().length;
        previousBatchId = binaryBatch.getBatchId();
        batchesQueue.add(binaryBatch);

        notify();

        return true;
    }

    @Override public synchronized BinaryBatch get() throws InterruptedException {
        while (true) {
            final BinaryBatch binaryBatch = batchesQueue.peek();
            if (binaryBatch != null) {
                return binaryBatch;
            }
            wait();
        }
    }

    @Override public synchronized void remove() {
        final BinaryBatch binaryBatch = batchesQueue.poll();
        if (binaryBatch != null) {
            occupiedBytes -= binaryBatch.getBytes().length;
        }
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return batchesQueue.isEmpty();
    }

    protected boolean isCapacityExceeded(BinaryBatch binaryBatch) throws InterruptedException {
        return maxBytes > 0 && occupiedBytes + binaryBatch.getBytes().length > maxBytes;
    }
}
