package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.util.Collection;

public class HeapStorage implements BatchStorage {

    @Override public boolean store(Collection<Action> actions, BatchFactory batchFactory) {
        return false;
    }

    @Override public BinaryBatch peek() {
        return null;
    }

    @Override public void remove() {

    }
}
