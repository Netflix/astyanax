package com.netflix.astyanax.cql.schema;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;


public class CqlKeyspaceDefinitionImpl implements KeyspaceDefinition {

	private final Cluster cluster; 
	private String keyspaceName; 
	private boolean alterKeyspace; 
	private Map<String, Object> options = new HashMap<String, Object>();
	
	public CqlKeyspaceDefinitionImpl(Cluster cluster) {
		this.cluster = cluster;
	}

	public CqlKeyspaceDefinitionImpl(Cluster cluster, Map<String, Object> input) {
		this.cluster = cluster;
		checkOptionsMap(input);
	}

	public CqlKeyspaceDefinitionImpl(Cluster cluster, Properties props) {
		this.cluster = cluster;
		checkOptionsMap(propertiesToMap(props));
	}

	public CqlKeyspaceDefinitionImpl(Cluster cluster, Row row) {
		
		this.cluster = cluster; 
		
		this.keyspaceName = row.getString("keyspace_name");
		this.setStrategyClass(row.getString("strategy_class"));
		
		throw new NotImplementedException();
		//parseStrategyOptions(row);
		//refreshProperties();
	}
	
	public CqlKeyspaceDefinitionImpl alterKeyspace() {
		alterKeyspace = true;
		return this;
	}


	@Override
	public CqlKeyspaceDefinitionImpl setName(String name) {
		this.keyspaceName = name; 
		return this;
	}

	@Override
	public String getName() {
		return keyspaceName;
	}

	@Override
	public CqlKeyspaceDefinitionImpl setStrategyClass(String strategyClass) {
		getOrCreateReplicationMap().put("class", strategyClass);
		return this;
	}

	@Override
	public String getStrategyClass() {
		return (String) getOrCreateReplicationMap().get("class");
	}

	@Override
	public CqlKeyspaceDefinitionImpl setStrategyOptions(Map<String, String> strategyOptions) {
		getOrCreateReplicationMap().putAll(strategyOptions);
		return this;
	}
	
	public CqlKeyspaceDefinitionImpl setStrategyOptionsMap(Map<String, Object> strategyOptions) {
		getOrCreateReplicationMap().putAll(strategyOptions);
		return this;
	}

	
	@Override
	public CqlKeyspaceDefinitionImpl addStrategyOption(String name, String value) {
		this.getOrCreateReplicationMap().put(name, value);
		return this;
	}

	@Override
	public Map<String, String> getStrategyOptions() {
		Map<String, String> map = new HashMap<String, String>();
		Map<String, Object> repMap = getOrCreateReplicationMap();
		for (String key : repMap.keySet()) {
			map.put(key, (String) repMap.get(key));
		}
		return map;
	}

	@Override
	public List<ColumnFamilyDefinition> getColumnFamilyList() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition getColumnFamily(String columnFamily) {
		
		Query query = QueryBuilder.select().all()
								  .from("system", "schema_columnfamilies")
								  .where(eq("keyspace_name", keyspaceName))
								  .and(eq("columnfamily_name", columnFamily));
		
		return new CqlColumnFamilyDefinitionImpl(cluster.connect().execute(query).one());
	}

	@Override
	public KeyspaceDefinition addColumnFamily(ColumnFamilyDefinition cfDef) {
		throw new NotImplementedException();
	}

	@Override
	public Collection<String> getFieldNames() {
		throw new NotImplementedException();
	}

	@Override
	public Object getFieldValue(String name) {
		throw new NotImplementedException();
	}

	@Override
	public KeyspaceDefinition setFieldValue(String name, Object value) {
		throw new NotImplementedException();
	}

	@Override
	public Collection<FieldMetadata> getFieldsMetadata() {
		throw new NotImplementedException();
	}

	@Override
	public void setFields(Map<String, Object> options) {
		throw new NotImplementedException();
	}

	@Override
	public Properties getProperties() throws Exception {
		throw new NotImplementedException();
	}

	@Override
	public void setProperties(Properties props) throws Exception {
		options = propertiesToMap(props);
	}
	
	
	public OperationResult<SchemaChangeResult> execute() {
		
		String query = getQuery();

		System.out.println("Query : " + query);
		return null;
		//return new CqlOperationResultImpl<SchemaChangeResult>(cluster.connect().execute(query), null);
	}

	
	private String getQuery() {
		
		String cmd = (alterKeyspace) ? "ALTER" : "CREATE";
		
		StringBuilder sb = new StringBuilder(cmd); 
		sb.append(" KEYSPACE ");
		sb.append(keyspaceName);
		
		Map<String, Object> replicationOptions = (Map<String, Object>) options.get("replication");
		appendReplicationOptions(sb, replicationOptions);
		
		Object durableWrites = options.get("durable_writes");
		if (durableWrites != null) {
			sb.append(" AND durable_writes = ").append(durableWrites);
		}
		return sb.toString();
	}
	
	private void appendReplicationOptions(StringBuilder sb, Map<String, Object> replicationOptions) {

		if (replicationOptions == null || replicationOptions.size() == 0) {
			throw new RuntimeException("Missing properties for 'replication'");
		}

		sb.append(" WITH replication = {" );

		Iterator<Entry<String, Object>> iter = replicationOptions.entrySet().iterator();
		
		while (iter.hasNext()) {
			
			Entry<String, Object> entry = iter.next();
			sb.append("'").append(entry.getKey()).append("' : '").append(entry.getValue()).append("'");
			if (iter.hasNext()) {
				sb.append(", ");
			}
		}
		
		sb.append("}");
	}
	
	
	private void checkOptionsMap(Map<String, Object> input) {
		
		Object strategyOptions = input.get("strategy_options");
		
		if (strategyOptions == null) {
			Preconditions.checkArgument(options.get("replication") != null, "Invalid CREATE KEYSPACE properties");
			options = new HashMap<String, Object>();
			options.putAll(input);
			
		} else {
			
			// this is an old style map. Convert to the new spec of CREATE KEYSPACE

			options = new HashMap<String, Object>();
			
			Map<String, Object> oldStrategyOptions = (Map<String, Object>) input.get("strategy_options");
			this.setStrategyOptionsMap(oldStrategyOptions);

			String strategyClass = (String) input.get("strategy_class");
			this.setStrategyClass(strategyClass);
		}
	}
	
	private Map<String, Object> getOrCreateReplicationMap() {
		Map<String, Object> replicationMap = (Map<String, Object>) options.get("replication");
		if (replicationMap == null) {
			replicationMap = new HashMap<String, Object>();
			options.put("replication", replicationMap);
		}
		return replicationMap;
	}
	
	private static Map<String, Object> propertiesToMap(Properties props) {
		Map<String, Object> root = Maps.newTreeMap();
		for (Entry<Object, Object> prop : props.entrySet()) {
			String[] parts = StringUtils.split((String)prop.getKey(), ".");
			Map<String, Object> node = root;
			for (int i = 0; i < parts.length - 1; i++) {
				if (!node.containsKey(parts[i])) {
					node.put(parts[i], new LinkedHashMap<String, Object>());
				}
				node = (Map<String, Object>)node.get(parts[i]);
			}
			node.put(parts[parts.length-1], (String)prop.getValue());
		}
		return root;
	}

	
//	
//	private void parseStrategyOptions(Row row) {
//		
//		String jStr = row.getString("strategy_options");
//		if (jStr == null || jStr.isEmpty()) {
//			return;
//		}
//		
//		try {
//			JSONObject json = new JSONObject(jStr);
//			Iterator<String> iter = json.keys();
//			while (iter.hasNext()) {
//				String key = iter.next();
//				Object obj = json.get(key);
//				strategyOptions.put(key, obj.toString());
//			}
//		} catch (JSONException e) {
//			throw new RuntimeException(e);
//		}
//	}
	


}
