package com.netflix.astyanax.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.codahale.metrics.MetricRegistryListener;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ConnectionPoolProxy.SeedHostListener;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.schema.CqlColumnDefinitionImpl;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.schema.CqlKeyspaceDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;

/**
 * Java Driver based impl of {@link Cluster} that implements ddl operations.
 * The class encapsulates a java driver cluster and session object to provide all the functionality. 
 *  
 * Note that due to the way the object is setup via AstyanaxContext and CqlFamilyFactory, it needs to implements 
 * a {@link SeedHostListener} so that it can construct the cluster and session object appropriately once the seed hosts
 * have been provided by the {@link HostSupplier} object
 *  
 * @author poberai
 */
public class CqlClusterImpl implements com.netflix.astyanax.Cluster, SeedHostListener {

	public volatile Cluster cluster;
	private volatile Session session;
	private final AstyanaxConfiguration astyanaxConfig; 
	private final KeyspaceTracerFactory tracerFactory; 
	private final Configuration javaDriverConfig; 
	private final ConnectionPoolMonitor cpMonitor;
	private final MetricRegistryListener metricsRegListener;
	
	public CqlClusterImpl(AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory, ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor cpMonitor) {
		this.astyanaxConfig = asConfig;
		this.tracerFactory = tracerFactory;
		this.javaDriverConfig = ((JavaDriverConnectionPoolConfigurationImpl)cpConfig).getJavaDriverConfig();
		this.cpMonitor = cpMonitor;
		this.metricsRegListener = ((JavaDriverConnectionPoolMonitorImpl)cpMonitor).getMetricsRegistryListener();
	}

	@Override
	public String describeClusterName() throws ConnectionException {
		return cluster.getMetadata().getClusterName();
	}

	@Override
	public String getVersion() throws ConnectionException {
		
		Statement query = QueryBuilder.select("release_version")
								  .from("system", "local")
								  .where(eq("key", "local"));
		
		return session.execute(query).one().getString("release_version"); 
	}

	public void shutdown() {
		cluster.close();
	}
	
	@Override
	public String describeSnitch() throws ConnectionException {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public String describePartitioner() throws ConnectionException {
		Statement query = QueryBuilder.select("partitioner")
				.from("system", "local")
				.where(eq("key", "local"));

		return session.execute(query).one().getString("partitioner"); 
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		return new CqlSchemaVersionReader(session).exec();
	}

	@Override
	public KeyspaceDefinition makeKeyspaceDefinition() {
        return new CqlKeyspaceDefinitionImpl(session);
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
		
		Statement query = QueryBuilder.select().all().from("system", "schema_keyspaces");

		List<KeyspaceDefinition> ksDefs = new ArrayList<KeyspaceDefinition>();
		try {
			for(Row row : session.execute(query).all()) {
				String keyspaceName = row.getString("keyspace_name");
				if (keyspaceName.equals("system") || keyspaceName.startsWith("system_")) {
					continue;
				}
				ksDefs.add(new CqlKeyspaceDefinitionImpl(session, row));
			}
			return ksDefs;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public KeyspaceDefinition describeKeyspace(String ksName) throws ConnectionException {
		return new CqlKeyspaceImpl(session, ksName, astyanaxConfig, tracerFactory,cpMonitor).describeKeyspace();
	}

	@Override
	public Keyspace getKeyspace(String keyspace) throws ConnectionException {
		return new CqlKeyspaceImpl(session, keyspace, astyanaxConfig, tracerFactory,cpMonitor);
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace(String keyspaceName) throws ConnectionException {
		return new CqlKeyspaceImpl(session, keyspaceName.toLowerCase(), astyanaxConfig, tracerFactory,cpMonitor).dropKeyspace();
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
		return new CqlKeyspaceDefinitionImpl(session, options).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Properties props) throws ConnectionException {
		String keyspaceName = (String) props.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(session, props).setName(keyspaceName).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException {
		String keyspaceName = (String) options.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(session, options).setName(keyspaceName).alterKeyspace().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException {
		String keyspaceName = (String) props.remove("name");
		if (keyspaceName == null) {
			throw new RuntimeException("Options missing 'name' property for keyspace name");
		}
		return new CqlKeyspaceDefinitionImpl(session, props).setName(keyspaceName).alterKeyspace().execute();
	}
	
	@Override
	public AstyanaxConfiguration getConfig() {
		return astyanaxConfig;
	}

	@Override
	public ColumnFamilyDefinition makeColumnFamilyDefinition() {
		return new CqlColumnFamilyDefinitionImpl(session);
	}

	@Override
	public ColumnDefinition makeColumnDefinition() {
		return new CqlColumnDefinitionImpl();
	}
	
	@Override
	public Properties getColumnFamilyProperties(String keyspace, String columnfamilyName) throws ConnectionException {
		try {
			return new CqlKeyspaceDefinitionImpl(session).setName(keyspace).getColumnFamily(columnfamilyName).getProperties();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, null, options).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, null, props).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, null, options).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException {
		return new CqlColumnFamilyDefinitionImpl(session, null, props).alterTable().execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(String keyspaceName, String columnFamilyName) throws ConnectionException {
		return new CqlKeyspaceImpl(session, keyspaceName, astyanaxConfig, tracerFactory,cpMonitor).dropColumnFamily(columnFamilyName);
	}

	@Override
	public OperationResult<SchemaChangeResult> addColumnFamily(ColumnFamilyDefinition def) throws ConnectionException {
		return ((CqlColumnFamilyDefinitionImpl)def).execute();
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamilyDefinition def) throws ConnectionException {
		return ((CqlColumnFamilyDefinitionImpl)def).alterTable().execute();
	}

	@Override
	public void setHosts(Collection<Host> hosts, int port) {
		
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
		
		Configuration config = javaDriverConfig;
		
		// We really need a mechanism to easily override Configuration on the builder
		Cluster.Builder builder = Cluster.builder()
				.addContactPoints(contactPoints.toArray(new String[0]))
				.withPort(port)
				.withLoadBalancingPolicy(config.getPolicies().getLoadBalancingPolicy())
				.withReconnectionPolicy(config.getPolicies().getReconnectionPolicy())
				.withRetryPolicy(config.getPolicies().getRetryPolicy())
				.withCompression(config.getProtocolOptions().getCompression())
				.withPoolingOptions(config.getPoolingOptions())
				.withSocketOptions(config.getSocketOptions())
				.withQueryOptions(config.getQueryOptions());
		
		if (config.getMetricsOptions() == null) {
			builder.withoutMetrics();
		} else if (!config.getMetricsOptions().isJMXReportingEnabled()) {
			builder.withoutJMXReporting();
		}
				
		this.cluster = builder.build(); 
		if (!(this.cpMonitor instanceof JavaDriverConnectionPoolMonitorImpl))
			this.cluster.getMetrics().getRegistry().addListener((MetricRegistryListener) this.metricsRegListener);
		this.session = cluster.connect();
	}
}
