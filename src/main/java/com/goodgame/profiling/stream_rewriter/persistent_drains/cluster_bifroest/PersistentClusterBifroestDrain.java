package com.goodgame.profiling.stream_rewriter.persistent_drains.cluster_bifroest;

import java.io.IOException;
import java.util.Collection;

import com.goodgame.profiling.bifroest.bifroest_client.BifroestClient;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrain;



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
