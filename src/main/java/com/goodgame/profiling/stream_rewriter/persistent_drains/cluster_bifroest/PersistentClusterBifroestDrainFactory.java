package com.goodgame.profiling.stream_rewriter.persistent_drains.cluster_bifroest;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.bifroest.bifroest_client.BifroestClientSystem;
import com.goodgame.profiling.bifroest.bifroest_client.EnvironmentWithBifroestClient;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrain;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrainFactory;


@MetaInfServices
public class PersistentClusterBifroestDrainFactory<E extends EnvironmentWithBifroestClient> implements PersistentDrainFactory<E, PersistentDrain> {

    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Arrays.asList( EnvironmentWithBifroestClient.class );
    }

    @Override
    public String handledType() {
        return "cluster-bifroest";
    }

    @Override
    public void addRequiredSystems( Collection<String> requiredSystems, JSONObject subconfiguration ) {
        requiredSystems.add( BifroestClientSystem.IDENTIFIER );
    }

    @Override
    public PersistentDrain create( E e, JSONObject jsono ) {
        return new PersistentClusterBifroestDrain( e.bifroestClient() );
    }
}
