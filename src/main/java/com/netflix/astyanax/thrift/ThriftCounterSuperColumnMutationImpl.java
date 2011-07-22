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
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class ThriftCounterSuperColumnMutationImpl<C> implements ColumnListMutation<C> {
	private final Clock clock;
	private final List<Mutation> mutationList;
	private final ColumnPath<C> path;
	private CounterSuperColumn superColumn;
	private SlicePredicate deletionPredicate;

	public ThriftCounterSuperColumnMutationImpl(Clock clock,
			List<Mutation> mutationList, 
			ColumnPath<C> path) {
		this.path = path;
		this.clock = clock;
		this.mutationList = mutationList;
	}
	
	@Override
	public <V> ColumnListMutation<C> putColumn(C columnName, V value,
			Serializer<V> valueSerializer, Integer ttl) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl) {
		return putColumn(columnName, value, StringSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl) {
		return putColumn(columnName, value, BytesArraySerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl) {
		return putColumn(columnName, value, IntegerSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl) {
		return putColumn(columnName, value, LongSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl) {
		return putColumn(columnName, value, BooleanSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl) {
		return putColumn(columnName, value, ByteBufferSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl) {
		return putColumn(columnName, value, DateSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl) {
		return putColumn(columnName, value, DoubleSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl) {
		return putColumn(columnName, value, UUIDSerializer.get(), ttl);
	}

	@Override
	public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
		return putColumn(columnName, null, UUIDSerializer.get(), ttl);
	}
	
	@Override
	public ColumnListMutation<C> delete() {
		// Delete the entire super column
		Deletion d = new Deletion()
			.setSuper_column(path.get(0))
			.setTimestamp(clock.getCurrentTime());
	    mutationList.add(new Mutation().setDeletion(d));
	    return this;
	}

	@Override
	public <SC> ColumnListMutation<SC> withSuperColumn(
			ColumnPath<SC> superColumnPath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
		// 1.  Set up the column with all the data
		CounterColumn column = new CounterColumn();
		column.setName(path.getSerializer().toByteBuffer(columnName));
		column.setValue(amount);
		
		// 2.  Create the super column mutation if this is the first call
		if (superColumn == null) {
			superColumn = new CounterSuperColumn().setName(path.get(0));
			
		    Mutation mutation = new Mutation();
		    mutation.setColumn_or_supercolumn(
		    		new ColumnOrSuperColumn().setCounter_super_column(superColumn));
		    mutationList.add(mutation);
		}	    
		superColumn.addToColumns(column);
		
		return this;
	}

	@Override
	public ColumnListMutation<C> deleteColumn(C columnName) {
		if (deletionPredicate == null) {
			deletionPredicate = new SlicePredicate();
			Deletion d = new Deletion()
				.setTimestamp(clock.getCurrentTime())
				.setSuper_column(path.get(0))
				.setPredicate(deletionPredicate);
			
		    mutationList.add(new Mutation().setDeletion(d));
		}
		
		deletionPredicate.addToColumn_names(path.getSerializer().toByteBuffer(columnName));
		return this;
	}
}
