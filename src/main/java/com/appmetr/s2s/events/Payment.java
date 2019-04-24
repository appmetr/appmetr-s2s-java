package com.appmetr.s2s.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Payment extends Action {
    private static final String ACTION = "trackPayment";

    private String orderId;
    private String transactionId;
    private String processor;
    private String psUserSpentCurrencyCode;
    private String psUserSpentCurrencyAmount;
    private String appCurrencyCode;
    private String appCurrencyAmount;
    private String psUserStoreCountryCode;
    @JsonProperty("isSandbox")
    private Boolean sandbox;

    //Only for Jackson deserialization
    private Payment() {
        super(ACTION);
    }

    public Payment(String orderId,
                   String transactionId,
                   String processor,
                   String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount) {
        this(orderId, transactionId, processor, psUserSpentCurrencyCode, psUserSpentCurrencyAmount, null, null);
    }

    public Payment(String orderId, String transactionId, String processor, String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount, String appCurrencyCode, String appCurrencyAmount) {
        this(orderId, transactionId, processor, psUserSpentCurrencyCode, psUserSpentCurrencyAmount,
                appCurrencyCode, appCurrencyAmount, null, null);
    }

    public Payment(String orderId,
                   String transactionId,
                   String processor,
                   String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount,
                   String appCurrencyCode,
                   String appCurrencyAmount,
                   String psUserStoreCountryCode,
                   Boolean sandbox) {
        this();

        this.orderId = orderId;
        this.transactionId = transactionId;
        this.processor = processor;
        this.psUserSpentCurrencyCode = psUserSpentCurrencyCode;
        this.psUserSpentCurrencyAmount = psUserSpentCurrencyAmount;
        this.appCurrencyCode = appCurrencyCode;
        this.appCurrencyAmount = appCurrencyAmount;
        this.psUserStoreCountryCode = psUserStoreCountryCode;
        this.sandbox = sandbox;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getProcessor() {
        return processor;
    }

    public String getPsUserSpentCurrencyCode() {
        return psUserSpentCurrencyCode;
    }

    public String getPsUserSpentCurrencyAmount() {
        return psUserSpentCurrencyAmount;
    }

    public String getAppCurrencyCode() {
        return appCurrencyCode;
    }

    public String getAppCurrencyAmount() {
        return appCurrencyAmount;
    }

    public String getPsUserStoreCountryCode() {
        return psUserStoreCountryCode;
    }

    public Boolean getSandbox() {
        return sandbox;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize()
                + getStringLength(orderId)
                + getStringLength(transactionId)
                + getStringLength(processor)
                + getStringLength(psUserSpentCurrencyCode)
                + getStringLength(psUserSpentCurrencyAmount)
                + getStringLength(appCurrencyCode)
                + getStringLength(appCurrencyAmount)
                + getStringLength(psUserStoreCountryCode);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Payment payment = (Payment) o;
        return Objects.equals(getOrderId(), payment.getOrderId()) &&
                Objects.equals(getTransactionId(), payment.getTransactionId()) &&
                Objects.equals(getProcessor(), payment.getProcessor()) &&
                Objects.equals(getPsUserSpentCurrencyCode(), payment.getPsUserSpentCurrencyCode()) &&
                Objects.equals(getPsUserSpentCurrencyAmount(), payment.getPsUserSpentCurrencyAmount()) &&
                Objects.equals(getAppCurrencyCode(), payment.getAppCurrencyCode()) &&
                Objects.equals(getAppCurrencyAmount(), payment.getAppCurrencyAmount()) &&
                Objects.equals(getPsUserStoreCountryCode(), payment.getPsUserStoreCountryCode()) &&
                Objects.equals(getSandbox(), payment.getSandbox());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getOrderId(), getTransactionId(), getProcessor(),
                getPsUserSpentCurrencyCode(), getPsUserSpentCurrencyAmount(), getAppCurrencyCode(),
                getAppCurrencyAmount(), getPsUserStoreCountryCode(), getSandbox());
    }

    @Override public String toString() {
        return "Payment{" +
                "orderId='" + getOrderId() + '\'' +
                ", transactionId='" + getTransactionId() + '\'' +
                ", processor='" + getProcessor() + '\'' +
                ", psUserSpentCurrencyCode='" + getPsUserSpentCurrencyCode() + '\'' +
                ", psUserSpentCurrencyAmount='" + getPsUserSpentCurrencyAmount() + '\'' +
                ", appCurrencyCode='" + getAppCurrencyCode() + '\'' +
                ", appCurrencyAmount='" + getAppCurrencyAmount() + '\'' +
                ", psUserStoreCountryCode='" + getPsUserStoreCountryCode() + '\'' +
                ", sandbox=" + getSandbox() +
                "} " + super.toString();
    }
}
