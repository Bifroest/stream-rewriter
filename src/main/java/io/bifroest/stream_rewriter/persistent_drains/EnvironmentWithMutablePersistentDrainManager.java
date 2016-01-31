package io.bifroest.stream_rewriter.persistent_drains;

import io.bifroest.commons.boot.interfaces.Environment;


public interface EnvironmentWithMutablePersistentDrainManager extends
		EnvironmentWithPersistentDrainManager {
	public void setPersistentDrainManager(PersistentDrainManager<? extends Environment> persistentDrainManager);
}
