package com.goodgame.profiling.stream_rewriter.persistent_drains;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.stream_rewriter.persistent_drains.empty.PersistentVoidDrain;

public class PersistentDrainManager<E extends EnvironmentWithJSONConfiguration> {
    private static final Logger log = LogManager.getLogger();

    private Map<String, PersistentDrain> drains = new HashMap<>();

    public PersistentDrainManager(
            E env,
            Map<String, PersistentDrainFactory<E, ? extends PersistentDrain>> factoriesByDrainId,
            Map<String, JSONObject> drainConfigsByDrainId
    ) {
        // always have a persistent void drain, to make overriding easier
        drains.put( "void", new PersistentVoidDrain() );

        for( String drainId : drainConfigsByDrainId.keySet() ) {
            JSONObject persistentDrainConfig = drainConfigsByDrainId.get( drainId );

            if ( factoriesByDrainId.containsKey( drainId ) ) {
                drains.put( drainId, factoriesByDrainId.get( drainId ).create( env, persistentDrainConfig ) );
                log.info( "Created PersistentDrain for type " + persistentDrainConfig.getString( "type" ) + " with id " + drainId );
            } else {
                log.warn( "No PersistentDrainFactory found for type " + persistentDrainConfig.getString( "type" ) + " while configuring id " + drainId );
            }
        }
    }

    public PersistentDrain getPersistentDrain( String id ) {
        return drains.get( id );
    }

    public boolean hasPersistentDrain( String id ) {
        return drains.containsKey( id );
    }

    public Map<String, PersistentDrain> getAllPersistentDrains() {
        return drains;
    }

    public void shutdown() {
        for( PersistentDrain drain : drains.values() ) {
            drain.shutdown();
        }
    }
}
