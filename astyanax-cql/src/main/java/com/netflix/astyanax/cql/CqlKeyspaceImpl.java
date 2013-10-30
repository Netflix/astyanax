package com.netflix.astyanax.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Clock;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.clock.MicrosecondsAsyncClock;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.CqlConnectionPoolProxy.SeedHostListener;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.direct.DirectCqlStatement;
import com.netflix.astyanax.cql.reads.CqlColumnFamilyQueryImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.schema.CqlKeyspaceDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.cql.writes.CqlColumnMutationImpl;
import com.netflix.astyanax.cql.writes.CqlMutationBatchImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.partitioner.BigInteger127Partitioner;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.UnknownComparatorException;

public class CqlKeyspaceImpl implements Keyspace, SeedHostListener {

	private static final Logger Logger = LoggerFactory.getLogger(CqlKeyspaceImpl.class);
	
	private final Clock clock;
	
	public volatile Session session;
	
	private final KeyspaceContext ksContext;
	private final String keyspaceName;
	private final AstyanaxConfiguration astyanaxConfig;
	private final KeyspaceTracerFactory tracerFactory; 

	public CqlKeyspaceImpl(Session session, String ksName, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		this.session = session;
		this.keyspaceName = ksName.toLowerCase();
		this.astyanaxConfig = asConfig;
		this.tracerFactory = tracerFactory;
		this.ksContext = new KeyspaceContext(this);
		
		if (asConfig.getClock() != null) {
			clock = asConfig.getClock();
		} else {
			clock = new MicrosecondsAsyncClock();
		}
	}
	
	public CqlKeyspaceImpl(KeyspaceContext ksContext) {
		this(ksContext.getSession(), ksContext.getKeyspace(), ksContext.getConfig(), ksContext.getTracerFactory());
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
		String pName = describePartitioner();
		if (pName.contains("Murmur3Partitioner")) {
			return Murmur3Partitioner.get();
		} else if (pName.contains("RandomPartitioner")) {
			return BigInteger127Partitioner.get();
		} else {
			throw new RuntimeException("Unrecognized partitioner: " + pName);
		}
	}

	@Override
	public String describePartitioner() throws ConnectionException {
		Query q = QueryBuilder.select("partitioner").from("system", "local");
		ResultSet result = session.execute(q);
		com.datastax.driver.core.Row row = result.one();
		if (row == null) {
			throw new RuntimeException("Missing paritioner");
		}
		String pName = row.getString(0);
		return pName;
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException {
		return CqlRingDescriber.getInstance().getTokenRanges(session, false);
	}

	@Override
	public List<TokenRange> describeRing(String dc) throws ConnectionException {
		return CqlRingDescriber.getInstance().getTokenRanges(session, dc, null);
	}

	@Override
	public List<TokenRange> describeRing(String dc, String rack) throws ConnectionException {
		return CqlRingDescriber.getInstance().getTokenRanges(session, dc, rack);
	}

	@Override
	public List<TokenRange> describeRing(boolean cached) throws ConnectionException {
		return CqlRingDescriber.getInstance().getTokenRanges(session, cached);
	}

	@Override
	public KeyspaceDefinition describeKeyspace() throws ConnectionException {
		
		Query query = QueryBuilder.select().from("system", "schema_keyspaces").where(eq("keyspace_name", keyspaceName));
		Row row = session.execute(query).one();
		if (row == null) {
			throw new RuntimeException("Keyspace not found: " + keyspaceName);
		}
		return (new CqlKeyspaceDefinitionImpl(session, row));
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
		
		ColumnFamilyDefinition cfDef = describeKeyspace().getColumnFamily(cfName);
		return new SerializerPackageImpl(cfDef, ignoreErrors);
	}

	@Override
	public MutationBatch prepareMutationBatch() {
		return new CqlMutationBatchImpl(ksContext, clock, astyanaxConfig.getDefaultWriteConsistencyLevel(), astyanaxConfig.getRetryPolicy());
	}

	@Override
	public <K, C> ColumnMutation prepareColumnMutation(ColumnFamily<K, C> columnFamily, K rowKey, C column) {
		return new CqlColumnMutationImpl<K,C>(ksContext, new ColumnFamilyMutationContext<K, C>(columnFamily, rowKey), column);
	}

	@Override
	public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
		return new CqlColumnFamilyQueryImpl<K,C>(ksContext, cf);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(session, options).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Properties properties) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(session, properties).setName(keyspaceName).execute();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options, Map<ColumnFamily, Map<String, Object>> cfs) throws ConnectionException {
		
		CqlKeyspaceDefinitionImpl ksDef = new CqlKeyspaceDefinitionImpl(session, options);
		if (ksDef.getName() == null) {
			ksDef.setName(keyspaceName);
		}
		
		OperationResult<SchemaChangeResult> result = ksDef.execute();
		
		for (ColumnFamily cf : cfs.keySet()) {
			CqlColumnFamilyDefinitionImpl cfDef = new CqlColumnFamilyDefinitionImpl(session, ksDef.getName(), cf, cfs.get(cf));
			ksDef.addColumnFamily(cfDef);
		}
		
		return result;
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(session, options).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException {
		return new CqlKeyspaceDefinitionImpl(session, props).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException {
		return new CqlOperationResultImpl<SchemaChangeResult>(session.execute("DROP KEYSPACE " + keyspaceName), null);
	}
	
	@Override
	public <K, C> OperationResult<Void> truncateColumnFamily(ColumnFamily<K, C> columnFamily) throws OperationException, ConnectionException {
		ResultSet result = session.execute("TRUNCATE " + keyspaceName + "." + columnFamily.getName());
		return new CqlOperationResultImpl<Void>(result, null);
	}

	@Override
	public OperationResult<Void> truncateColumnFamily(String columnFamily) throws ConnectionException {
		ResultSet result = session.execute("TRUNCATE " + keyspaceName + "." + columnFamily);
		return new CqlOperationResultImpl<Void>(result, null);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, columnFamily, options).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, props).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, options).execute();
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, columnFamily, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, props).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, keyspaceName, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(String columnFamilyName) throws ConnectionException {
		return new CqlOperationResultImpl<SchemaChangeResult>(session.execute("DROP TABLE " + keyspaceName + "." + columnFamilyName), null);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(ColumnFamily<K, C> columnFamily) throws ConnectionException {
		return dropColumnFamily(columnFamily.getName());
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		return new CqlSchemaVersionReader(session).exec();
	}

	@Override
	public CqlStatement prepareCqlStatement() {
		return new DirectCqlStatement(session);
	}

	@Override
	public ConnectionPool<?> getConnectionPool() throws ConnectionException {
		throw new UnsupportedOperationException("Operation not supported");
	}


	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation) throws ConnectionException {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation, RetryPolicy retry) throws ConnectionException {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public void setHosts(Collection<Host> hosts, int port) {

		try {
			if (session != null) {
				Logger.info("Session has already been set, SKIPPING SET HOSTS");
				return;
			}
			List<Host> hostList = Lists.newArrayList(hosts);

			List<String> contactPoints = Lists.transform(hostList, new Function<Host, String>() {
				@Override
				public String apply(Host input) {
					if (input != null) {
						return input.getHostName(); 
					}
					return null;
				}
			});

			Cluster cluster = Cluster.builder()
					.addContactPoints(contactPoints.toArray(new String[0]))
					.withPort(port)
					.build();
			
			session = cluster.connect();

		} catch (RuntimeException e) {
			Logger.error("Failed to set hosts for keyspace impl", e);
		} catch (Exception e) {
			Logger.error("Failed to set hosts for keyspace impl", e);
		}
	}

	public class KeyspaceContext {
		
		private final Keyspace ks; 
		
		public KeyspaceContext(Keyspace keyspaceCtx) {
			this.ks = keyspaceCtx;
		}
		public Session getSession() {
			return session;
		}
		public String getKeyspace() {
			return keyspaceName;
		}
		public AstyanaxConfiguration getConfig() {
			return astyanaxConfig;
		}
		public KeyspaceTracerFactory getTracerFactory() {
			return tracerFactory;
		}
		
		public Keyspace getKeyspaceContext() {
			return ks;
		}
	}
}
