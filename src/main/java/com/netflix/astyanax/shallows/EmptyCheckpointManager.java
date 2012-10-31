/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
