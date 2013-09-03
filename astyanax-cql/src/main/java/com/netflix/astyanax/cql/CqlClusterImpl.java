package com.netflix.astyanax.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.CqlConnectionPoolProxy.SeedHostListener;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.schema.CqlKeyspaceDefinitionImpl;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;

public class CqlClusterImpl implements com.netflix.astyanax.Cluster, SeedHostListener {

	public volatile Cluster cluster = null;
	private volatile ChainedContext context;
	private final AstyanaxConfiguration astyanaxConfig; 
	private final KeyspaceTracerFactory tracerFactory; 
	
	public CqlClusterImpl(AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		this.astyanaxConfig = asConfig;
		this.tracerFactory = tracerFactory;
	}

	public CqlClusterImpl(Cluster cluster, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
		this.cluster = cluster;
		this.context = new ChainedContext(tracerFactory).add(cluster); 
		this.astyanaxConfig = asConfig;
		this.tracerFactory = tracerFactory;
	}
	
	@Override
	public String describeClusterName() throws ConnectionException {
		return cluster.getMetadata().getClusterName();
	}

	@Override
	public String getVersion() throws ConnectionException {
		
		
		Query query = QueryBuilder.select("release_version")
								  .from("system", "local")
								  .where(eq("key", "local"));
		
		return cluster.connect().execute(query).one().getString("release_version"); 
	}

	public void shutdown() {
		cluster.shutdown();
	}
	
	@Override
	public String describeSnitch() throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public String describePartitioner() throws ConnectionException {
		Query query = QueryBuilder.select("partitioner")
				.from("system", "local")
				.where(eq("key", "local"));

		return cluster.connect().execute(query).one().getString("partitioner"); 
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		Query query = QueryBuilder.select("schema_version")
				.from("system", "local")
				.where(eq("key", "local"));
		
		String localVersion = cluster.connect().execute(query).one().getString("schema_version");
		
		Map<String, List<String>> map = Collections.emptyMap();
		List<String> localList = Collections.emptyList(); 
		localList.add(localVersion);
		map.put(localVersion, localList);
		
		query = QueryBuilder.select("schema_version")
				.from("system", "peers")
				.where(eq("key", "local"));
		
		String peerVersion = cluster.connect().execute(query).one().getString("schema_version");
		List<String> peerList = Collections.emptyList(); 
		peerList.add(peerVersion);
		map.put(peerVersion, peerList);
		
		return map;
	}


	@Override
	public KeyspaceDefinition makeKeyspaceDefinition() {
        return new CqlKeyspaceDefinitionImpl(cluster);
	}

	@Override
	public Properties getAllKeyspaceProperties() throws ConnectionException {

		Properties properties = new Properties();
		try {
			List<KeyspaceDefinition> ksDefs = describeKeyspaces();
			for(KeyspaceDefinition ksDef : ksDefs) {
				Properties ksProps = ksDef.getProperties();
				for (Object key : ksProps.keySet()) {
					properties.put(ksDef.getName() + "." + key, ksProps.get(key));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return properties;
	}

	@Override
	public Properties getKeyspaceProperties(String keyspace) throws ConnectionException {
		
		try {
			return describeKeyspace(keyspace.toLowerCase()).getProperties();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<KeyspaceDefinition> describeKeyspaces() throws ConnectionException {
		
		Query query = QueryBuilder.select().all().from("system", "schema_keyspaces");

		List<KeyspaceDefinition> ksDefs = new ArrayList<KeyspaceDefinition>();
		try {
			for(Row row : cluster.connect().execute(query).all()) {
				ksDefs.add(new CqlKeyspaceDefinitionImpl(cluster, row));
			}
			return ksDefs;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public KeyspaceDefinition describeKeyspace(String ksName) throws ConnectionException {

		Query query = QueryBuilder.select().from("system", "schema_keyspaces").where(eq("keyspace_name", ksName.toLowerCase()));

		try {
			return (new CqlKeyspaceDefinitionImpl(cluster, cluster.connect().execute(query).one()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}	
	}

	@Override
	public Keyspace getKeyspace(String keyspace) throws ConnectionException {
		return new CqlKeyspaceImpl(cluster, keyspace, astyanaxConfig, tracerFactory);
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace(String keyspaceName) throws ConnectionException {
		return new CqlOperationResultImpl<SchemaChangeResult>(
				cluster.connect().execute("DROP KEYSPACE " + keyspaceName.toLowerCase()), null);
	}

	@Override
	public OperationResult<SchemaChangeResult> addKeyspace(KeyspaceDefinition def) throws ConnectionException {
		return ((CqlKeyspaceDefinitionImpl)def).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(KeyspaceDefinition def) throws ConnectionException {
		return ((CqlKeyspaceDefinitionImpl)def).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException {
		String keyspaceName = (String) options.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(cluster, options).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Properties props) throws ConnectionException {
		String keyspaceName = (String) props.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(cluster, props).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException {
		String keyspaceName = (String) options.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(cluster, options).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException {
		String keyspaceName = (String) props.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(cluster, props).setName(keyspaceName).alterKeyspace().execute();
	}
	
	@Override
	public AstyanaxConfiguration getConfig() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition makeColumnFamilyDefinition() {
		return new CqlColumnFamilyDefinitionImpl(cluster);
	}

	@Override
	public ColumnDefinition makeColumnDefinition() {
		throw new NotImplementedException();
	}
	
	@Override
	public Properties getColumnFamilyProperties(String keyspace, String keyspaces) throws ConnectionException {
		throw new NotImplementedException();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, null, options).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, null, props).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, null, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(cluster, null, props).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(String keyspaceName, String columnFamilyName) throws ConnectionException {
		
		return new CqlOperationResultImpl<SchemaChangeResult>(
				cluster.connect().execute("DROP TABLE " + keyspaceName + "." + columnFamilyName), null);
	}

	@Override
	public OperationResult<SchemaChangeResult> addColumnFamily(ColumnFamilyDefinition def) throws ConnectionException {
		return ((CqlColumnFamilyDefinitionImpl)def).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamilyDefinition def) throws ConnectionException {
		return ((CqlColumnFamilyDefinitionImpl)def).alterTable().execute();
	}
	
	private static KeyspaceTracerFactory DefaultNoOpTracerFactory = new  KeyspaceTracerFactory() {

		@Override
		public CassandraOperationTracer newTracer(CassandraOperationType type) { 
			return DefaultNoOpCassandraOpTracer; 
		}

		@Override
		public CassandraOperationTracer newTracer(CassandraOperationType type, ColumnFamily<?, ?> columnFamily) { 
			return DefaultNoOpCassandraOpTracer; 
		}
	};
	
	private static CassandraOperationTracer DefaultNoOpCassandraOpTracer = new CassandraOperationTracer() {

		@Override
		public CassandraOperationTracer start() { return this; }

		@Override
		public void success() {}

		@Override
		public void failure(ConnectionException e) {}
	};

	@Override
	public void setHosts(Collection<Host> hosts) {
		
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
		
		this.cluster = Cluster.builder().addContactPoints(contactPoints.toArray(new String[0])).build();
		this.context = new ChainedContext(tracerFactory).add(cluster); 
	}
	
	
}
