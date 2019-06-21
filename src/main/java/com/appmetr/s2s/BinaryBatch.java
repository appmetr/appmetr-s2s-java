package com.appmetr.s2s;

public class BinaryBatch {
    private final long batchId;
    private final byte[] bytes;

    public BinaryBatch(long batchId, byte[] bytes) {
        this.batchId = batchId;
        this.bytes = bytes;
    }

    public long getBatchId() {
        return batchId;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override public String toString() {
        return "BinaryBatch{" +
                "batchId=" + batchId +
                '}';
    }
}
