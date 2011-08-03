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
import java.util.Arrays;
import java.util.Date;

import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.IndexColumnExpression;
import com.netflix.astyanax.query.IndexOperationExpression;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.IndexValueExpression;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public abstract class AbstractIndexQueryImpl<K,C> implements IndexQuery<K,C> {
	protected final org.apache.cassandra.thrift.IndexClause indexClause =
		new org.apache.cassandra.thrift.IndexClause();
	protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.RANGE_ALL);
	private ColumnFamily<K, C> columnFamily;
	
	public AbstractIndexQueryImpl(ColumnFamily<K, C> columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Override
	public IndexQuery<K, C> withColumnSlice(C... columns) {
    	predicate.setColumn_names(columnFamily.getColumnSerializer()
    			.toBytesList(Arrays.asList(columns))).setSlice_rangeIsSet(false);
		return this;
	}

	@Override
	public IndexQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
		if (slice.getColumns() != null) {
        	predicate.setColumn_names(columnFamily.getColumnSerializer()
        		.toBytesList(slice.getColumns())).setSlice_rangeIsSet(false);
		}
		else {
			predicate.setSlice_range(ThriftUtils.createSliceRange(
					columnFamily.getColumnSerializer(),
					slice.getStartColumn(), 
					slice.getEndColumn(), 
					slice.getReversed(), 
					slice.getLimit()));
		}
		return this;
	}
	@Override
	public IndexQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		predicate.setSlice_range(ThriftUtils.createSliceRange(
				columnFamily.getColumnSerializer(),
				startColumn, endColumn, reversed, count));
		return this;
	}
	
	@Override
	public IndexQuery<K, C> withColumnRange(ByteBufferRange range) {
		predicate.setSlice_range(
				new SliceRange()
					.setStart(range.getStart())
					.setFinish(range.getEnd())
					.setCount(range.getLimit())
					.setReversed(range.isReversed()));
		return this;
	}
	
	@Override
	public IndexQuery<K, C> withColumnRange(ByteBuffer startColumn,
			ByteBuffer endColumn, boolean reversed, int count) {
		predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
		return this;
	}
	
	@Override
	public IndexQuery<K, C> setLimit(int count) {
		indexClause.setCount(count);
		return this;
	}

	@Override
	public IndexQuery<K, C> setStartKey(K key) {
		indexClause.setStart_key(columnFamily.getKeySerializer().toByteBuffer(key));
		return this;
	}

	private IndexQuery<K,C> getThisQuery() {
		return this;
	}
	
	static interface IndexExpression<K,C> extends 
		IndexColumnExpression<K,C>, 
		IndexOperationExpression<K,C>, 
		IndexValueExpression<K,C>
	{
	
	}
	
	@Override
	public IndexColumnExpression<K, C> addExpression() {
		return new IndexExpression<K,C>() {
			private final org.apache.cassandra.thrift.IndexExpression
				internalExpression = new org.apache.cassandra.thrift.IndexExpression();

			@Override
			public IndexOperationExpression<K, C> whereColumn(C columnName) {
				internalExpression.setColumn_name(columnFamily.getColumnSerializer().toBytes(columnName));
				return this;
			}

			@Override
			public IndexValueExpression<K, C> equals() {
				internalExpression.setOp(IndexOperator.EQ);
				return this;
			}

			@Override
			public IndexValueExpression<K, C> greaterThan() {
				internalExpression.setOp(IndexOperator.GT);
				return this;
			}

			@Override
			public IndexValueExpression<K, C> lessThan() {
				internalExpression.setOp(IndexOperator.LT);
				return this;
			}

			@Override
			public IndexValueExpression<K, C> greaterThanEquals() {
				internalExpression.setOp(IndexOperator.GTE);
				return this;
			}

			@Override
			public IndexValueExpression<K, C> lessThanEquals() {
				internalExpression.setOp(IndexOperator.LTE);
				return this;
			}

			@Override
			public IndexQuery<K, C> value(String value) {
				internalExpression.setValue(StringSerializer.get().toBytes(value));
				indexClause.addToExpressions(internalExpression);
				return getThisQuery();
			}

			@Override
			public IndexQuery<K, C> value(long value) {
				internalExpression.setValue(LongSerializer.get().toBytes(value));
				indexClause.addToExpressions(internalExpression);
				return getThisQuery();
			}

			@Override
			public IndexQuery<K, C> value(int value) {
				internalExpression.setValue(IntegerSerializer.get().toBytes(value));
				indexClause.addToExpressions(internalExpression);
				return getThisQuery();
			}

			@Override
			public IndexQuery<K, C> value(boolean value) {
				internalExpression.setValue(BooleanSerializer.get().toBytes(value));
				indexClause.addToExpressions(internalExpression);
				return getThisQuery();
			}

			@Override
			public IndexQuery<K, C> value(Date value) {
				internalExpression.setValue(DateSerializer.get().toBytes(value));
				indexClause.addToExpressions(internalExpression);
				return getThisQuery();
			}
		};
	}
}
