package io.bifroest.stream_rewriter.persistent_drains;

import java.time.Duration;
import java.time.Instant;

import io.bifroest.commons.util.panic.PanicAction;

public class DumpPersistentDrainStatus<E extends EnvironmentWithPersistentDrainManager> implements PanicAction {
    private E env;

    public DumpPersistentDrainStatus( E env ) {
        this.env = env;
    }

    @Override
    public void execute( Instant now ) {
        for( PersistentDrain drain : env.persistentDrainManager().getAllPersistentDrains().values() ) {
            drain.dumpInfos();
        }
    }

    @Override
    public Duration getCooldown() {
        return Duration.ofMinutes( 1 );
    }
}
