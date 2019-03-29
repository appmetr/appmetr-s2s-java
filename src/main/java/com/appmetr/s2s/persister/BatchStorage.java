package com.appmetr.s2s.persister;

public interface BatchStorage {

    /**
     * Inserts the specified element into this storage, waiting if necessary
     * for space to become available.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    void put(byte[] batch) throws InterruptedException;

    /**
     * Inserts the specified element into this storage if it is possible to do
     * so immediately without violating capacity restrictions, returning
     * {@code true} upon success and {@code false} if no space is currently
     * available.
     * @return {@code true} if the element was added to this storage, else
     *         {@code false}
     */
    boolean offer(byte[] batch);

    /**
     * Retrieves, but does not remove, the head of this storage, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this storage
     * @throws InterruptedException if interrupted while waiting
     */
    byte[] peek() throws InterruptedException;

    /**
     * Removes the head of this storage.
     */
     void remove();
}
