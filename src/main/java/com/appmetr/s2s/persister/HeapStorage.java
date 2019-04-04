package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HeapStorage implements BatchStorage {

    protected BlockingQueue<BinaryBatch> batchQueue = new LinkedBlockingQueue<>();
    protected Clock clock = Clock.systemUTC();
    protected long maxBytes = -1;

    protected long previousBatchId;
    protected long occupiedBytes;

    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) {
        long batchId = clock.millis();
        if (batchId <= previousBatchId) {
            batchId = previousBatchId + 1;
        }

        final BinaryBatch binaryBatch = batchFactory.createBatch(actions, batchId);
        if (occupiedBytes + binaryBatch.getBytes().length > maxBytes) {
            return false;
        }

        occupiedBytes += binaryBatch.getBytes().length;
        batchQueue.add(binaryBatch);

        return true;
    }

    @Override public BinaryBatch peek() {
        return batchQueue.peek();
    }

    @Override public void remove() {

    }
}
