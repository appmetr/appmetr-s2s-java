package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NonBlockingHeapStorage extends AbstractHeapStorage {

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
