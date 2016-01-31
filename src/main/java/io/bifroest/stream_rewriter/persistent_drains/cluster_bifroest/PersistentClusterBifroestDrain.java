package io.bifroest.stream_rewriter.persistent_drains.cluster_bifroest;

import java.io.IOException;
import java.util.Collection;

import io.bifroest.bifroest_client.BifroestClient;
import io.bifroest.commons.model.Metric;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrain;

public final class PersistentClusterBifroestDrain implements PersistentDrain {

    private final BifroestClient client;

    public PersistentClusterBifroestDrain( BifroestClient client ) {
        this.client = client;
    }

    @Override
    public void output( Collection<Metric> metrics ) throws IOException {
        this.client.sendMetrics( metrics );
    }

    @Override
    public void shutdown() {
        // nop.
    }

    @Override
    public void dumpInfos() {
        // nop.
    }
}
