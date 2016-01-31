package io.bifroest.stream_rewriter.persistent_drains;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithPersistentDrainManager extends Environment {
	public PersistentDrainManager<? extends Environment> persistentDrainManager();
}
