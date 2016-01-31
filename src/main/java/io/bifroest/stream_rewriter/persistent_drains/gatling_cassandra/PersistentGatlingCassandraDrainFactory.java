package com.goodgame.profiling.stream_rewriter.persistent_drains.gatling_cassandra;

import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.statistics.units.parse.DurationParser;
import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrainFactory;

@MetaInfServices
public class PersistentGatlingCassandraDrainFactory<E extends EnvironmentWithRetentionStrategy> implements
        PersistentDrainFactory<E, PersistentGatlingCassandraDrain<E>> {

    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Arrays.<Class<? super E>> asList( EnvironmentWithRetentionStrategy.class );
    }

    @Override
    public String handledType() {
        return "gatling-cassandra";
    }

    @Override
    public PersistentGatlingCassandraDrain<E> create( E environment, JSONObject config ) {
        DurationParser parser = new DurationParser();

        return new PersistentGatlingCassandraDrain<>(
                environment,
                JSONUtils.getWithDefault( config, "username", (String) null ),
                JSONUtils.getWithDefault( config, "password", (String) null ),
                JSONUtils.getStringArray( "seeds", config ),
                config.getString( "keyspace" ),
                config.optInt( "retained-prepared-statements", 64 ),
                parser.parse( config.optString( "initial-backoff", "500ms" ) ),
                parser.parse( config.optString( "maximum-backoff", "1m" ) ) );
    }

}
