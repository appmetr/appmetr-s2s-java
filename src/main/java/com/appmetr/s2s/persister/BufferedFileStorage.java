package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.io.IOException;
import java.util.Collection;

public class BufferedFileStorage implements BatchStorage {
    protected final FileStorage fileStorage;
    protected final AbstractHeapStorage heapStorage;
    protected final Thread saveThread;

    protected long batchId;
    protected volatile Throwable lastStorageThrowable;
    protected volatile boolean stopped;

    public BufferedFileStorage(FileStorage fileStorage, AbstractHeapStorage heapStorage) {
        this.fileStorage = fileStorage;
        this.heapStorage = heapStorage;
        batchId = fileStorage.lastBatchId;

        lastStorageThrowable = null;
        saveThread = new Thread(this::save, "appmetr-storage-" + fileStorage.path.getParent().getFileName());
        saveThread.setUncaughtExceptionHandler((t, e) -> lastStorageThrowable = e);
        saveThread.start();
    }

    @Override public synchronized boolean store(Collection<Action> actions, BatchFactory batchFactory) throws InterruptedException, IOException {
        if (stopped) {
            throw new IllegalStateException("Storage is in shutdown state", lastStorageThrowable);
        }

        if (lastStorageThrowable != null) {
            if (lastStorageThrowable instanceof IOException) {
                throw (IOException) lastStorageThrowable;
            } if (lastStorageThrowable instanceof Error) {
                throw (Error) lastStorageThrowable;
            } if (lastStorageThrowable instanceof RuntimeException) {
                throw (RuntimeException) lastStorageThrowable;
            } else {
                throw new RuntimeException(lastStorageThrowable);
            }
        }

        final boolean stored = heapStorage.store(batchFactory.createBatch(actions, batchId + 1));
        if (stored) {
            batchId++;
        }

        return stored;
    }

    protected void save() {
        while (true) {
            try {
                final BinaryBatch binaryBatch = heapStorage.peek();
                try {
                    fileStorage.store(binaryBatch);
                    heapStorage.remove();
                } catch (IOException e) {
                    lastStorageThrowable = e;
                    break;
                }
            } catch (InterruptedException e) {
                if (heapStorage.isEmpty()) {
                    break;
                }
            }

            if (stopped && heapStorage.isEmpty()) {
                break;
            }
        }
    }

    @Override public BinaryBatch peek() throws InterruptedException, IOException {
        return fileStorage.peek();
    }

    @Override public void remove() throws IOException {
        fileStorage.remove();
    }

    @Override public boolean isPersistent() {
        return true;
    }

    @Override public boolean isEmpty() {
        return fileStorage.isEmpty();
    }

    @Override public synchronized void shutdown() throws InterruptedException {
        stopped = true;
        saveThread.interrupt();
        saveThread.join();
    }

    public Throwable getLastStorageError() {
        return lastStorageThrowable;
    }
}
