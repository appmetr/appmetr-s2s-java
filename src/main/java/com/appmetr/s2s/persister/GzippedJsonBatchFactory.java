package com.appmetr.s2s.persister;

import com.appmetr.s2s.Batch;
import com.appmetr.s2s.BinaryBatch;
import com.appmetr.s2s.SerializationUtils;
import com.appmetr.s2s.events.Action;

import java.util.Collection;

public class GzippedJsonBatchFactory implements BatchFactoryServerId {
    public static final GzippedJsonBatchFactory instance = new GzippedJsonBatchFactory();

    protected GzippedJsonBatchFactory() {}

    @Override public BinaryBatch createBatch(Collection<Action> actions, long batchId, String serverId) {
        final Batch batch = new Batch(serverId, batchId, actions);
        return new BinaryBatch(batchId, SerializationUtils.serializeJsonGzip(batch, false));
    }
}
