package com.appmetr.s2s.persister;

import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.events.Action;

import java.util.Collection;

@FunctionalInterface
public interface BatchFactoryServerId {

    BinaryBatch createBatch(Collection<Action> actions, long batchId, String serverId);
}
