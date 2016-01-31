package io.bifroest.stream_rewriter.persistent_drains;

import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.commons.decorating_factories.BasicFactory;

public interface PersistentDrainFactory<E extends Environment, T extends PersistentDrain>
// BasicFactory, not BasicDrainFactory! We don't want these to get picked up by the "normal" serviceloader calls!
extends BasicFactory<E,T> {
}
