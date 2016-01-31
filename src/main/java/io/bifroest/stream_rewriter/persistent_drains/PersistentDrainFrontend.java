package io.bifroest.stream_rewriter.persistent_drains;

import java.io.IOException;
import java.util.List;

import io.bifroest.commons.model.Metric;
import io.bifroest.commons.util.panic.ProfilingPanic;
import io.bifroest.drains.AbstractBasicDrain;

public class PersistentDrainFrontend extends AbstractBasicDrain {

    private final PersistentDrain inner;

    public PersistentDrainFrontend( PersistentDrain inner ) {
        this.inner = inner;
    }

    @Override
    public void output( List<Metric> metrics ) throws IOException {
        try {
            inner.output( metrics );
        } catch ( RuntimeException | IOException e ) {
            ProfilingPanic.INSTANCE.panic();
            throw( e );
        }
    }
}
