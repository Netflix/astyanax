package com.netflix.astyanax.shallows;

import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.query.CheckpointManager;

public class EmptyCheckpointManager implements CheckpointManager {

	/**
	 * Do nothing since checkpoints aren't being persisted.
	 */
	@Override
	public void trackCheckpoint(String startToken, String checkpointToken) {
	}

	/**
	 * Since no checkpoint management is done here simply use the startToken as the 
	 * start checkpoint for the range
	 */
	@Override
	public String getCheckpoint(String startToken) {
		return startToken;
	}

	@Override
	public SortedMap<String, String> getCheckpoints() throws ConnectionException {
		return Maps.newTreeMap();
	}

}
