package io.bifroest.stream_rewriter.persistent_drains.empty;

import java.util.Collections;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrainFactory;

@MetaInfServices
public class PersistentVoidDrainFactory<E extends Environment> implements PersistentDrainFactory<E, PersistentVoidDrain> {
    @Override
    public List<Class<? super E>> getRequiredEnvironments() {
        return Collections.emptyList();
    }

    @Override
    public String handledType() {
        return "void";
    }

    @Override
    public PersistentVoidDrain create( E environment, JSONObject subconfiguration ) {
        return new PersistentVoidDrain();
    }
}
