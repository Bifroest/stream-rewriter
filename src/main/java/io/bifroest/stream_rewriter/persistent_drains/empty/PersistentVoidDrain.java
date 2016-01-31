package io.bifroest.stream_rewriter.persistent_drains.empty;

import java.io.IOException;
import java.util.Collection;

import io.bifroest.commons.model.Metric;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrain;

public class PersistentVoidDrain implements PersistentDrain {
    @Override
    public void output( Collection<Metric> metrics ) throws IOException {
        // intentionally empty
    }

    @Override
    public void shutdown() {
        // intentionally empty
    }

    @Override
    public void dumpInfos() {
        // intentionally empty
    }
}
