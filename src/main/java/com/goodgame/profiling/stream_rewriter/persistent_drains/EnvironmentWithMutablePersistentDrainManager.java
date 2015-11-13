package com.goodgame.profiling.stream_rewriter.persistent_drains;

import com.goodgame.profiling.commons.boot.interfaces.Environment;


public interface EnvironmentWithMutablePersistentDrainManager extends
		EnvironmentWithPersistentDrainManager {
	public void setPersistentDrainManager(PersistentDrainManager<? extends Environment> persistentDrainManager);
}
