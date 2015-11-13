package com.goodgame.profiling.stream_rewriter.persistent_drains.bifroest;

import java.util.Collections;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.boot.interfaces.Environment;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrainFactory;

@MetaInfServices
public class PersistentBifroestDrainFactory<E extends Environment> implements PersistentDrainFactory<E, PersistentBifroestDrain> {
    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Collections.emptyList();
    }

    @Override
    public String handledType() {
        return "bifroest";
    }

    @Override
    public PersistentBifroestDrain create( E environment, JSONObject subconfiguration ) {
        return new PersistentBifroestDrain(
                subconfiguration.getString( "host" ),
                subconfiguration.getInt( "port" ) );
    }
}
