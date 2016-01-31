package com.goodgame.profiling.stream_rewriter.persistent_drains;

import com.goodgame.profiling.commons.boot.interfaces.Environment;
import com.goodgame.profiling.commons.decorating_factories.BasicFactory;

public interface PersistentDrainFactory<E extends Environment, T extends PersistentDrain>
// BasicFactory, not BasicDrainFactory! We don't want these to get picked up by the "normal" serviceloader calls!
extends BasicFactory<E,T> {
}
