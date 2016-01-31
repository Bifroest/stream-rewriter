package com.goodgame.profiling.stream_rewriter.persistent_drains;

import java.io.IOException;
import java.util.Collection;

import com.goodgame.profiling.commons.model.Metric;

public interface PersistentDrain {

    void output( Collection<Metric> metrics ) throws IOException;

    void shutdown();

    void dumpInfos();
}
