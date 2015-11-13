package com.goodgame.profiling.stream_rewriter.persistent_drains;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.drains.BasicDrainFactory;
import com.goodgame.profiling.drains.Drain;
import com.goodgame.profiling.stream_rewriter.StreamRewriterIdentifiers;

@MetaInfServices
public class PersistentDrainFrontendFactory<E extends EnvironmentWithPersistentDrainManager> implements BasicDrainFactory<E> {
    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Arrays.<Class<? super E>> asList( EnvironmentWithPersistentDrainManager.class );
    }

    @Override
    public void addRequiredSystems( Collection<String> requiredSystems, JSONObject subconfiguration ) {
        requiredSystems.add( StreamRewriterIdentifiers.PERSISTENT_DRAINS );
    }

    @Override
    public String handledType() {
        return "persistent";
    }

    @Override
    public Drain create( E environment, JSONObject config, String name ) {
        return new PersistentDrainFrontend( environment.persistentDrainManager().getPersistentDrain( config.getString( "id" ) ) );
    }
}
