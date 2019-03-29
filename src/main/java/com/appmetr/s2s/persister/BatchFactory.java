package com.appmetr.s2s.persister;

import com.appmetr.s2s.events.Action;

import java.util.Collection;
import java.util.function.Function;

public class BatchFactory implements Function<Collection<Action>, byte[]> {

    @Override public byte[] apply(Collection<Action> actions) {
        return new byte[0];
    }
}
