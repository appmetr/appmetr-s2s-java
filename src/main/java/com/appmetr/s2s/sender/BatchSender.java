package com.appmetr.s2s.sender;

@FunctionalInterface
public interface BatchSender {

    boolean send(String uri, String token, byte[] batches);
}
