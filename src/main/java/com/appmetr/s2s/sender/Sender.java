package com.appmetr.s2s.sender;

@FunctionalInterface
public interface Sender {

    boolean send(String httpURL, String token, byte[] batches);
}
