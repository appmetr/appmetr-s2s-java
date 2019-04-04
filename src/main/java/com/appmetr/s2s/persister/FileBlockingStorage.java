package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.io.IOException;
import java.util.Collection;

public class FileBlockingStorage implements BatchStorage {

    @Override public boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
        return false;
    }

    @Override public BinaryBatch peek() throws InterruptedException {
        return null;
    }

    @Override public void remove() {

    }

    @Override public void init() throws IOException {

    }
}
