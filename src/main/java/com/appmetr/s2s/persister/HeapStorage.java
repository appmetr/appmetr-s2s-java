package com.appmetr.s2s.persister;

public class HeapStorage implements BatchStorage {

    @Override public void put(byte[] batch) throws InterruptedException {

    }

    @Override public boolean offer(byte[] batch) {
        return false;
    }

    @Override public byte[] peek() throws InterruptedException {
        return new byte[0];
    }

    @Override public void remove() {

    }
}
