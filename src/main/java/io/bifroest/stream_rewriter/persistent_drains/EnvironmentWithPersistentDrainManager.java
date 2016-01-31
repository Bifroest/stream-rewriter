package com.goodgame.profiling.stream_rewriter.persistent_drains;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithPersistentDrainManager extends Environment {
	public PersistentDrainManager<? extends Environment> persistentDrainManager();
}
