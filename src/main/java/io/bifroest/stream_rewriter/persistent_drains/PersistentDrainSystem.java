package io.bifroest.stream_rewriter.persistent_drains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.util.panic.ProfilingPanic;
import io.bifroest.stream_rewriter.StreamRewriterIdentifiers;

@MetaInfServices
public class PersistentDrainSystem<E extends EnvironmentWithMutablePersistentDrainManager & EnvironmentWithJSONConfiguration> implements Subsystem<E> {
    private static final Logger log = LogManager.getLogger();

    private Map<String, PersistentDrainFactory<E, ? extends PersistentDrain>> factoriesByDrainId;
    private Map<String, JSONObject> drainConfigsByDrainId;

    @Override
    public String getSystemIdentifier() {
        return StreamRewriterIdentifiers.PERSISTENT_DRAINS;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        Collection<String> requiredSystems = new ArrayList<>( Arrays.asList( SystemIdentifiers.RETENTION ) );

        for ( String drainId : factoriesByDrainId.keySet() ) {
            factoriesByDrainId.get( drainId ).addRequiredSystems( requiredSystems, drainConfigsByDrainId.get( drainId ) );
        }

        return requiredSystems;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void configure( JSONObject configuration ) {
        JSONObject config = configuration.getJSONObject( "persistent drains" );

        Iterable<PersistentDrainFactory> factories = ServiceLoader.load( PersistentDrainFactory.class );
        drainConfigsByDrainId = new HashMap<>();
        factoriesByDrainId = new HashMap<>();

        drainConfigLoop:
        for( String drainId : config.keySet() ) {
            JSONObject drainConfig = config.getJSONObject( drainId );
            drainConfigsByDrainId.put( drainId, drainConfig );

            for( PersistentDrainFactory<E, ? extends PersistentDrain> factory : factories ) {
                if ( factory.handledType().equals( drainConfig.getString( "type" ) ) ) {
                    factoriesByDrainId.put( drainId, factory );
                    continue drainConfigLoop;
                }
            }
            log.warn( "No PersistentDrainFactory found for type {} while configuring id {}",
                    drainConfig.getString( "type" ),
                    drainId );
        }
    }

    @Override
    public void boot(E env) throws Exception {
        ProfilingPanic.INSTANCE.addAction(new DumpPersistentDrainStatus<>(env));
        env.setPersistentDrainManager( new PersistentDrainManager<>( env, factoriesByDrainId, drainConfigsByDrainId ) );
    }

    @Override
    public void shutdown(E env) {
        env.persistentDrainManager().shutdown();
    }
}
