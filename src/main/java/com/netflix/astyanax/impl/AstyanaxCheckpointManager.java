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
package com.netflix.astyanax.impl;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.CheckpointManager;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Track checkpoints in cassandra
 * 
 * @author elandau
 *
 */
public class AstyanaxCheckpointManager implements CheckpointManager {

	private final ByteBuffer bbKey;
	private final Keyspace keyspace;
	private final ColumnFamily<ByteBuffer, String> columnFamily;
	
    @SuppressWarnings("rawtypes")
    private final static Comparator tokenComparator = new Comparator() {
        @Override
        public int compare(Object arg0, Object arg1) {
        	return new BigInteger((String)arg0).compareTo(new BigInteger((String)arg1));
        }
    };

	public AstyanaxCheckpointManager(Keyspace keyspace, String columnFamily, String id) {
		this(keyspace, columnFamily, StringSerializer.get().toByteBuffer(id));
	}
	
	public AstyanaxCheckpointManager(Keyspace keyspace, String columnFamily, Long id) {
		this(keyspace, columnFamily, LongSerializer.get().toByteBuffer(id));
	}
	
	public AstyanaxCheckpointManager(Keyspace keyspace, String columnFamily, ByteBuffer bbKey) {
		this.keyspace = keyspace;
		this.bbKey = bbKey;
		this.columnFamily = ColumnFamily.newColumnFamily(columnFamily, ByteBufferSerializer.get(), StringSerializer.get());
	}
	
	@Override
	public void trackCheckpoint(String startToken, String checkpointToken) throws ConnectionException {
		keyspace.prepareColumnMutation(columnFamily,  bbKey,  startToken).putValue(checkpointToken, null).execute();
	}

	@Override
	public String getCheckpoint(String startToken) throws ConnectionException {
		try {
			return keyspace.prepareQuery(columnFamily).getKey(bbKey).getColumn(startToken).execute().getResult().getStringValue();
		}
		catch (NotFoundException e) {
			return startToken;
		}
	}

	@Override
	public SortedMap<String, String> getCheckpoints() throws ConnectionException {
		SortedMap<String, String> checkpoints = Maps.newTreeMap(tokenComparator);
		for (Column<String> column : keyspace.prepareQuery(columnFamily).getKey(bbKey).execute().getResult()) {
			checkpoints.put(column.getName(), column.getStringValue());
		}
		
		return checkpoints;
	}

}
