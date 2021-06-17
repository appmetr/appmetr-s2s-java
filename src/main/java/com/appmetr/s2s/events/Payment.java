package com.appmetr.s2s.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Payment extends Action {
    public static final String ACTION = "trackServerPayment";

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
    private String paymentClientIp;

    //Only for Jackson deserialization
    private Payment() {
        super(ACTION);
    }

    public Payment(String orderId,
                   String transactionId,
                   String processor,
                   String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount,
                   String paymentClientIp) {
        this(orderId, transactionId, processor, psUserSpentCurrencyCode, psUserSpentCurrencyAmount, null, null, paymentClientIp);
    }

    public Payment(String orderId, String transactionId, String processor, String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount, String appCurrencyCode, String appCurrencyAmount, String paymentClientIp) {
        this(orderId, transactionId, processor, psUserSpentCurrencyCode, psUserSpentCurrencyAmount,
                appCurrencyCode, appCurrencyAmount, null, null, paymentClientIp);
    }

    public Payment(String orderId,
                   String transactionId,
                   String processor,
                   String psUserSpentCurrencyCode,
                   String psUserSpentCurrencyAmount,
                   String appCurrencyCode,
                   String appCurrencyAmount,
                   String psUserStoreCountryCode,
                   Boolean sandbox,
                   String paymentClientIp) {
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
        this.paymentClientIp = paymentClientIp;
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

    public String getPaymentClientIp() {
        return paymentClientIp;
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
                + getStringLength(psUserStoreCountryCode)
                + getStringLength(paymentClientIp);
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
                Objects.equals(getSandbox(), payment.getSandbox()) &&
                Objects.equals(getPaymentClientIp(), payment.getPaymentClientIp());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getOrderId(), getTransactionId(), getProcessor(),
                getPsUserSpentCurrencyCode(), getPsUserSpentCurrencyAmount(), getAppCurrencyCode(),
                getAppCurrencyAmount(), getPsUserStoreCountryCode(), getSandbox(), getPaymentClientIp());
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
                ", paymentClientIp='" + getPaymentClientIp() + '\'' +
                "} " + super.toString();
    }
}
