package com.appmetr.s2s.persister;

import com.appmetr.s2s.events.Action;

import java.util.Collection;

@FunctionalInterface
public interface BatchFactory {

    byte[] createBatch(Collection<Action> actions, long batchId);
}
