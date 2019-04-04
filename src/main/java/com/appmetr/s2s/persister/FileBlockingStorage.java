package com.appmetr.s2s.persister;

import com.appmetr.s2s.events.Action;

import java.io.IOException;
import java.util.Collection;

public class FileBlockingStorage implements BatchStorage {

    @Override public boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException {
        return false;
    }

    @Override public byte[] peek() throws InterruptedException {
        return new byte[0];
    }

    @Override public void remove() {

    }

    @Override public void init() throws IOException {

    }
}
