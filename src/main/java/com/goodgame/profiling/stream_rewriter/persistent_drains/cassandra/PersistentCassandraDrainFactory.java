package com.goodgame.profiling.stream_rewriter.persistent_drains.cassandra;

import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrainFactory;

@MetaInfServices
public class PersistentCassandraDrainFactory<E extends EnvironmentWithRetentionStrategy> implements
        PersistentDrainFactory<E, PersistentCassandraDrain<E>> {

    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Arrays.<Class<? super E>> asList( EnvironmentWithRetentionStrategy.class );
    }

    @Override
    public String handledType() {
        return "cassandra";
    }

    @Override
    public PersistentCassandraDrain<E> create( E environment, JSONObject config ) {
        return new PersistentCassandraDrain<E>(
                environment,
                JSONUtils.getWithDefault( config, "username", (String) null ),
                JSONUtils.getWithDefault( config, "password", (String) null ),
                JSONUtils.getStringArray( "seeds", config ),
                config.getString( "keyspace" ) );
    }

}
