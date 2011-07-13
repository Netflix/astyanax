package com.netflix.astyanax.thrift;

import com.netflix.astyanax.Query;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;

public abstract class AbstractQueryImpl<K, C, R> implements Query<K, C, R> {
	protected ConsistencyLevel consistencyLevel;
	protected long timeout;
	protected ColumnPath<C> path;
	protected ColumnSlice<C> slice;
	
	public AbstractQueryImpl(ColumnPath<C> path, ConsistencyLevel consistencyLevel, long timeout) {
		this.consistencyLevel = consistencyLevel;
		this.timeout = timeout;
		this.path = path;
	}
	
	@Override
	public Query<K,C,R> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public Query<K,C,R> setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	@Override
	public Query<K,C,R> setColumnPath(ColumnPath<C> path) {
		this.path = path;
		return this;
	}

	@Override
	public Query<K,C,R> setColumnSlice(ColumnSlice<C> slice) {
		this.slice = slice;
		return this;
	}
}
