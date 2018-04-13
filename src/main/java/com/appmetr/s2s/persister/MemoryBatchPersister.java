package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.events.Action;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

public class MemoryBatchPersister implements BatchPersister {

    protected Queue<Batch> batchQueue = new ArrayDeque<>();
    protected String serverId = UUID.randomUUID().toString();
    protected long previousBatchId;
    protected int batchesLimit;
    protected Clock clock = Clock.systemUTC();

    @Override public Batch getNext() {
        synchronized (batchQueue) {
            return batchQueue.peek();
        }
    }

    @Override public boolean persist(List<Action> actionList) {
        synchronized (batchQueue) {
            if (batchesLimit > 0 && batchQueue.size() >= batchesLimit) {
                return false;
            }

            long batchId = clock.millis();
            if (batchId <= previousBatchId) {
                batchId = previousBatchId + 1;
            }

            previousBatchId = batchId;
            final Batch batch = new Batch(serverId, batchId, actionList);
            batchQueue.add(batch);

            return true;
        }
    }

    @Override public void remove() {
        synchronized (batchQueue) {
            batchQueue.poll();
        }
    }

    @Override public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public void setBatchesLimit(int batchesLimit) {
        this.batchesLimit = batchesLimit;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }
}
