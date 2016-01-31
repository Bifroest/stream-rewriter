package io.bifroest.stream_rewriter.persistent_drains;

import java.io.IOException;
import java.util.Collection;

import io.bifroest.commons.model.Metric;

public interface PersistentDrain {

    void output( Collection<Metric> metrics ) throws IOException;

    void shutdown();

    void dumpInfos();
}
