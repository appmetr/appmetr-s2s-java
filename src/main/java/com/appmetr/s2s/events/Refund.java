package com.appmetr.s2s.events;

import java.util.Objects;

public class Refund extends Action {
    public static final String ACTION = "trackRefundInfo";

    private String transactionId;
    private long cancellationDateMs;

    //Only for Jackson deserialization
    private Refund() {
        super(ACTION);
    }

    public Refund(String transactionId, long cancellationDateMs) {
        this();
        this.transactionId = transactionId;
        this.cancellationDateMs = cancellationDateMs;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public long getCancellationDateMs() {
        return cancellationDateMs;
    }

    public void setCancellationDateMs(long cancellationDateMs) {
        this.cancellationDateMs = cancellationDateMs;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize()
                + getStringLength(transactionId)
                + Long.BYTES;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Refund refund = (Refund) o;
        return Objects.equals(transactionId, refund.getTransactionId()) && cancellationDateMs == refund.cancellationDateMs;
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getTransactionId(), cancellationDateMs);
    }

    @Override
    public String toString() {
        return "Refund{" +
                "transactionId='" + transactionId + '\'' +
                ", cancellationDateMs=" + cancellationDateMs +
                '}' + super.toString();
    }
}
