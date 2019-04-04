package com.appmetr.s2s.persister;

import com.appmetr.s2s.events.Action;

import java.util.Collection;

public class GzippedJsonBatchFactory implements BatchFactoryServerId {

    @Override public byte[] createBatch(Collection<Action> actions, long batchId, String serverId) {
        return new byte[0];
    }
}
