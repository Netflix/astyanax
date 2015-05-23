package com.netflix.astyanax.cql.schema;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;

/**
 * Impl for {@link KeyspaceDefinition} using the java driver. 
 * 
 * @author poberai
 *
 */
public class CqlKeyspaceDefinitionImpl implements KeyspaceDefinition {

	private static final Logger Log = LoggerFactory.getLogger(CqlKeyspaceDefinitionImpl.class);
	
	private final Session session; 
	private boolean alterKeyspace; 
	private final Map<String, Object> options = new HashMap<String, Object>();
	private final List<CqlColumnFamilyDefinitionImpl> cfDefList = new ArrayList<CqlColumnFamilyDefinitionImpl>();
	
	public CqlKeyspaceDefinitionImpl(Session session) {
		this.session = session;
	}

	public CqlKeyspaceDefinitionImpl(Session session, Map<String, Object> input) {
		this.session = session;
		checkOptionsMap(input);
	}

	public CqlKeyspaceDefinitionImpl(Session session, Properties props) {
		this.session = session;
		checkOptionsMap(propertiesToMap(props));
	}

	public CqlKeyspaceDefinitionImpl(Session session, Row row) {
		
		this.session = session; 
		this.setName(row.getString("keyspace_name"));
		this.setStrategyClass(row.getString("strategy_class"));
		this.setStrategyOptionsMap(parseStrategyOptions(row.getString("strategy_options")));
		this.options.put("durable_writes", row.getBool("durable_writes"));
	}
	
	public CqlKeyspaceDefinitionImpl alterKeyspace() {
		alterKeyspace = true;
		return this;
	}


	@Override
	public CqlKeyspaceDefinitionImpl setName(String name) {
		this.options.put("name", name.toLowerCase());
		return this;
	}

	@Override
	public String getName() {
		return (String) options.get("name");
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
		Statement query = QueryBuilder.select().all()
				.from("system", "schema_columnfamilies")
				.where(eq("keyspace_name", getName()));
				
		ResultSet rs = session.execute(query);
		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		List<Row> rows = rs.all();
		if (rows != null) {
			for (Row row : rows) {
				cfDefs.add(new CqlColumnFamilyDefinitionImpl(session, row));
			}
		}
		return cfDefs;
	}

	@Override
	public ColumnFamilyDefinition getColumnFamily(String columnFamilyName) {
		
		Statement query = QueryBuilder.select().all()
				.from("system", "schema_columnfamilies")
				.where(eq("keyspace_name", getName()))
				.and(eq("columnfamily_name", columnFamilyName.toLowerCase()));

		Row row = session.execute(query).one();
		
		if (row == null) {
			throw new RuntimeException("CF not found: " + columnFamilyName);
		}
		return new CqlColumnFamilyDefinitionImpl(session, row);
	}

	@Override
	public KeyspaceDefinition addColumnFamily(ColumnFamilyDefinition cfDef) {
		CqlColumnFamilyDefinitionImpl cqlCfDef = (CqlColumnFamilyDefinitionImpl) cfDef; 
		cqlCfDef.execute();
		return this;
	}

	@Override
	public Collection<String> getFieldNames() {
		return options.keySet();
	}

	@Override
	public Object getFieldValue(String name) {
		return options.get(name);
	}

	@Override
	public KeyspaceDefinition setFieldValue(String name, Object value) {
		this.options.put(name, value);
		return this;
	}

	@Override
	public Collection<FieldMetadata> getFieldsMetadata() {
		
		List<FieldMetadata> list = new ArrayList<FieldMetadata>();
		
		for (String key : options.keySet()) {
			Object value = options.get(key);
			
			Class<?> clazz = value.getClass();
			
			String name = key.toUpperCase();
			String type = clazz.getSimpleName().toUpperCase();
			boolean isContainer = Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz);
			list.add(new FieldMetadata(name, type, isContainer));
		}
		return list;
	}

	@Override
	public void setFields(Map<String, Object> optionsMap) {
		checkOptionsMap(optionsMap);
	}

	@Override
	public Properties getProperties() throws Exception {
		return mapToProperties(options);
	}

	@Override
	public void setProperties(Properties props) throws Exception {
		options.clear();
		options.putAll(propertiesToMap(props));
	}
	
	
	public OperationResult<SchemaChangeResult> execute() {
		
		String query = getQuery();

		if (Log.isDebugEnabled()) {
			Log.debug("Query : " + query);
		}
		
		CqlOperationResultImpl<SchemaChangeResult> result = new CqlOperationResultImpl<SchemaChangeResult>(session.execute(query), null);
		
		for (CqlColumnFamilyDefinitionImpl cfDef : cfDefList) {
			cfDef.execute();
		}
		
		return result;
	}

	
	private String getQuery() {
		
		String cmd = (alterKeyspace) ? "ALTER" : "CREATE";
		
		StringBuilder sb = new StringBuilder(cmd); 
		sb.append(" KEYSPACE ");
		sb.append(getName());
		
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
			Preconditions.checkArgument(input.get("replication") != null, "Invalid CREATE KEYSPACE properties");
			options.clear();
			options.putAll(input);
			
		} else {
			
			// this is an old style map. Convert to the new spec of CREATE KEYSPACE
			options.clear();
			
			Map<String, Object> replicationOptions = new HashMap<String, Object>();
			options.put("replication", replicationOptions);
			
			Map<String, Object> oldStrategyOptions = (Map<String, Object>) input.get("strategy_options");
			replicationOptions.putAll(oldStrategyOptions);

			String strategyClass = (String) input.get("strategy_class");
			replicationOptions.put("class", strategyClass);
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


	private static Properties mapToProperties(Map<String, Object> map) {
		
		Properties props = new Properties();
		addProperties(props, null, map);
		return props;
	}
	
	private static void addProperties(Properties props, String prefix, Map<String, Object> subMap) {
		
		for (Entry<String, Object> entry : subMap.entrySet()) {
			
			String key = (prefix != null) ? prefix + "." + entry.getKey() : entry.getKey();
			if (entry.getValue() instanceof Map) {
				addProperties(props, key, (Map<String, Object>) entry.getValue());
			} else {
				props.put(key, entry.getValue().toString());
			}
		}
	}
	
	private Map<String, Object> parseStrategyOptions(String jsonString) {
		
		if (jsonString == null || jsonString.isEmpty()) {
			return null;
		}
		
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			JSONObject json = new JSONObject(jsonString);
			Iterator<String> iter = json.keys();
			while (iter.hasNext()) {
				String key = iter.next();
				Object obj = json.get(key);
				map.put(key, obj);
			}
			return map;
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
	

	public String toString() {
		return "CqlKeyspaceDefinition=[ " + options.toString() + " ]";
	}

}
