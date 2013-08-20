package com.netflix.astyanax.cql;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.clock.MicrosecondsAsyncClock;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.reads.CqlColumnFamilyQueryImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.schema.CqlKeyspaceDefinitionImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.cql.util.Context.ColumnFamilyCtx;
import com.netflix.astyanax.cql.writes.CqlColumnMutationImpl;
import com.netflix.astyanax.cql.writes.CqlMutationBatchImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.UnknownComparatorException;

public class CqlKeyspaceImpl implements Keyspace {

	private MicrosecondsAsyncClock microsClock = new MicrosecondsAsyncClock();
	
	private String keyspaceName;
	private AstyanaxConfiguration astyanaxConfig;
	private Cluster cluster;
	
	private ChainedContext context; 
	
	public CqlKeyspaceImpl(String name, Cluster cluster) {
		this.keyspaceName = name;
		this.cluster = cluster;
	}
	
	public CqlKeyspaceImpl(ChainedContext ctx) {
		this.context = ctx;
		this.context.rewindForRead();
		
		this.cluster = context.getNext(Cluster.class);
		this.keyspaceName = context.getNext(String.class);
	}

	@Override
	public AstyanaxConfiguration getConfig() {
		return astyanaxConfig;
	}

	@Override
	public String getKeyspaceName() {
		return keyspaceName;
	}

	@Override
	public Partitioner getPartitioner() throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public String describePartitioner() throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public List<TokenRange> describeRing(String dc) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public List<TokenRange> describeRing(String dc, String rack) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public List<TokenRange> describeRing(boolean cached) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public KeyspaceDefinition describeKeyspace() throws ConnectionException {
		return new CqlClusterImpl(cluster).describeKeyspace(keyspaceName);
	}

	@Override
	public Properties getKeyspaceProperties() throws ConnectionException {
		try {
			return describeKeyspace().getProperties();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Properties getColumnFamilyProperties(String columnFamily) throws ConnectionException {
        KeyspaceDefinition ksDef = this.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(columnFamily);
        if (cfDef == null)
            throw new NotFoundException(String.format("Column family '%s' in keyspace '%s' not found", columnFamily, getKeyspaceName()));
        try {
			return cfDef.getProperties();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public SerializerPackage getSerializerPackage(String cfName, boolean ignoreErrors) throws ConnectionException, UnknownComparatorException {
		throw new NotImplementedException();
	}

	@Override
	public MutationBatch prepareMutationBatch() {
		return new CqlMutationBatchImpl(cluster, keyspaceName, microsClock, null, null);
	}

	@Override
	public <K, C> ColumnMutation prepareColumnMutation(ColumnFamily<K, C> columnFamily, K rowKey, C column) {
		return new CqlColumnMutationImpl(new ColumnFamilyCtx(cluster, keyspaceName, columnFamily), String.valueOf(column), rowKey);
	}

	@Override
	public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
		return new CqlColumnFamilyQueryImpl<K,C>(context.clone().add(cf));
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(cluster, options).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Properties properties) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(cluster, properties).setName(keyspaceName).execute();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options, Map<ColumnFamily, Map<String, Object>> cfs) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(cluster, options).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(cluster, props).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException {
		return new CqlOperationResultImpl<SchemaChangeResult>(
				cluster.connect().execute("DROP KEYSPACE " + keyspaceName), null);
	}
	
	@Override
	public <K, C> OperationResult<Void> truncateColumnFamily(ColumnFamily<K, C> columnFamily) throws OperationException, ConnectionException {
		ResultSet result = cluster.connect().execute("TRUNCATE " + keyspaceName + "." + columnFamily.getName());
		return new CqlOperationResultImpl<Void>(result, null);
	}

	@Override
	public OperationResult<Void> truncateColumnFamily(String columnFamily) throws ConnectionException {
		ResultSet result = cluster.connect().execute("TRUNCATE " + keyspaceName + "." + columnFamily);
		return new CqlOperationResultImpl<Void>(result, null);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, columnFamily, options).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, props).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, options).execute();
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, columnFamily, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, props).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(String columnFamilyName) throws ConnectionException {
		return new CqlOperationResultImpl<SchemaChangeResult>(
				cluster.connect().execute("DROP TABLE " + keyspaceName + "." + columnFamilyName), null);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(ColumnFamily<K, C> columnFamily) throws ConnectionException {
		return dropColumnFamily(columnFamily.getName());
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public CqlStatement prepareCqlStatement() {
		throw new NotImplementedException();
	}

	@Override
	public ConnectionPool<?> getConnectionPool() throws ConnectionException {
		throw new NotImplementedException();
	}


	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation, RetryPolicy retry) throws ConnectionException {
		throw new NotImplementedException();
	}

}
