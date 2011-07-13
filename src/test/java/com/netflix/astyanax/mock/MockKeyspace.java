package com.netflix.astyanax.mock;

import java.util.List;

import com.netflix.astyanax.CounterMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Query;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.KeySlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.model.TokenRange;
import com.netflix.astyanax.query.ColumnFamilyQuery;

public class MockKeyspace implements Keyspace {
	private String keyspaceName;
	private List<TokenRange> tokenRange;
	
	public MockKeyspace(String name) {
		this.keyspaceName = name;
	}
	
	public void setTokenRange(List<TokenRange> tokens) {
		this.tokenRange = tokens;
	}
	
	@Override
	public String getKeyspaceName() {
		return this.keyspaceName;
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException {
		return this.tokenRange;
	}

	@Override
	public MutationBatch prepareMutationBatch() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, ColumnList<C>> prepareGetRowQuery(
			ColumnFamily<K, ?> columnFamily, Serializer<C> columnSerializer,  K rowKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, Rows<K, C>> prepareGetMultiRowQuery(
			ColumnFamily<K, ?> columnFamily, Serializer<C> columnSerializer, KeySlice<K> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> Query<K, C, Column<C>> prepareGetColumnQuery(
			ColumnFamily<K, ?> columnFamily, K rowKey, ColumnPath<C> path) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> CounterMutation<K, C> prepareCounterMutation(
			ColumnFamily<K, C> columnFamily, K rowKey, ColumnPath<C> path,
			long amount) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void start() {
	}

	@Override
	public <K, C> Query<K, C, Rows<K, C>> prepareCqlQuery(String cql) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
		// TODO Auto-generated method stub
		return null;
	}
}
