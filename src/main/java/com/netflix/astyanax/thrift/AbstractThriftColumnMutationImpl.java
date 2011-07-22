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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public abstract class AbstractThriftColumnMutationImpl implements ColumnMutation {

	protected final ByteBuffer key;
	protected final ByteBuffer column;
	protected ConsistencyLevel readConsistencyLevel;
	protected ConsistencyLevel writeConsistencyLevel;
	protected final Clock clock;
	
	public AbstractThriftColumnMutationImpl(ByteBuffer key, ByteBuffer column, Clock clock, ConsistencyLevel readCl, ConsistencyLevel writeCl) {
		this.key = key;
		this.column = column;
		this.readConsistencyLevel = readCl;
		this.writeConsistencyLevel = writeCl;
		this.clock = clock;
	}
	
	@Override
	public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		readConsistencyLevel = consistencyLevel;
		writeConsistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public Execution<Void> putValue(String value, Integer ttl) {
		return insertValue(StringSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(byte[] value, Integer ttl) {
		return insertValue(BytesArraySerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(int value, Integer ttl) {
		return insertValue(IntegerSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(long value, Integer ttl) {
		return insertValue(LongSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(boolean value, Integer ttl) {
		return insertValue(BooleanSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(ByteBuffer value, Integer ttl) {
		return insertValue(ByteBufferSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(Date value, Integer ttl) {
		return insertValue(DateSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(double value, Integer ttl) {
		return insertValue(DoubleSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putValue(UUID value, Integer ttl) {
		return insertValue(UUIDSerializer.get().toByteBuffer(value), ttl);
	}

	@Override
	public Execution<Void> putEmptyColumn(Integer ttl) {
		return insertValue(ThriftUtils.EMPTY_BYTE_BUFFER, ttl);
	}

	public abstract Execution<Void> insertValue(ByteBuffer value, Integer ttl);

}
