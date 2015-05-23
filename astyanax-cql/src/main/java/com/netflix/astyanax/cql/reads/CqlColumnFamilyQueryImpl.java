package com.netflix.astyanax.cql.reads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Base impl for {@link ColumnFamilyQuery} interface. This class is the root for all read operations in Astyanax.
 * From this class, we can branch into either {@link RowQuery} or {@link RowSliceQuery}. 
 *  
 * The current class manages the column family context, retry policy and the consistency level for the read queries underneath.
 *  
 *  Important classes to see are
 *  {@link CqlRowQueryImpl}
 *  {@link CqlRowSliceQueryImpl}
 *  {@link CqlAllRowsQueryImpl}
 *  
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlColumnFamilyQueryImpl<K, C> implements ColumnFamilyQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<K,C> cfContext;
	
	private boolean useCaching = false;
	
	public CqlColumnFamilyQueryImpl(KeyspaceContext ksCtx, ColumnFamily<K,C> cf) {
		this.ksContext = ksCtx;
		this.cfContext = new CFQueryContext<K,C>(cf);
		this.cfContext.setConsistencyLevel(ConsistencyLevel.CL_ONE);
	}
	
	@Override
	public ColumnFamilyQuery<K, C> setConsistencyLevel(ConsistencyLevel clLevel) {
		this.cfContext.setConsistencyLevel(clLevel);
		return this;
	}

	@Override
	public ColumnFamilyQuery<K, C> withRetryPolicy(RetryPolicy retry) {
		this.cfContext.setRetryPolicy(retry.duplicate());
		return this;
	}

	@Override
	public ColumnFamilyQuery<K, C> pinToHost(Host host) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public RowQuery<K, C> getKey(K rowKey) {
		return new CqlRowQueryImpl<K, C>(ksContext, cfContext, rowKey, useCaching);
	}

	@Override
	public RowQuery<K, C> getRow(K rowKey) {
		return new CqlRowQueryImpl<K, C>(ksContext, cfContext, rowKey, useCaching);
	}

	@Override
	public RowSliceQuery<K, C> getKeyRange(K startKey, K endKey, String startToken, String endToken, int count) {
		return getRowRange(startKey, endKey, startToken, endToken, count);
	}

	@Override
	public RowSliceQuery<K, C> getRowRange(K startKey, K endKey, String startToken, String endToken, int count) {
		CqlRowSlice<K> rowSlice = new CqlRowSlice<K>(startKey, endKey, startToken, endToken, count);
		return new CqlRowSliceQueryImpl<K, C>(ksContext, cfContext, rowSlice, useCaching);
	}

	@Override
	public RowSliceQuery<K, C> getKeySlice(K... keys) {
		return getRowSlice(keys);
	}

	@Override
	public RowSliceQuery<K, C> getRowSlice(K... keys) {
		List<K> keyList = Arrays.asList(keys);
		return getRowSlice(keyList);
	}

	@Override
	public RowSliceQuery<K, C> getKeySlice(Collection<K> keys) {
		return getRowSlice(keys);
	}

	@Override
	public RowSliceQuery<K, C> getRowSlice(Collection<K> keys) {
		CqlRowSlice<K> rowSlice = new CqlRowSlice<K>(keys);
		return new CqlRowSliceQueryImpl<K, C>(ksContext, cfContext, rowSlice, useCaching);
	}

	@Override
	public RowSliceQuery<K, C> getKeySlice(Iterable<K> keys) {
		return getRowSlice(keys);
	}

	@Override
	public RowSliceQuery<K, C> getRowSlice(Iterable<K> keys) {
		List<K> keyList = new ArrayList<K>();
		for (K key : keys) {
			keyList.add(key);
		}
		return getRowSlice(keyList);
	}

	@Override
	public AllRowsQuery<K, C> getAllRows() {
		return new CqlAllRowsQueryImpl<K, C>(ksContext.getKeyspaceContext(), cfContext.getColumnFamily());
	}

	@Override
	public CqlQuery<K, C> withCql(String cql) {
		return new DirectCqlQueryImpl<K, C>(ksContext, cfContext, cql);
	}

	@Override
	public IndexQuery<K, C> searchWithIndex() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyQuery<K, C> withCaching(boolean condition) {
		this.useCaching = condition;
		return this;
	}
}
