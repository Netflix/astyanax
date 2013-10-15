package com.netflix.astyanax.cql.schema;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;


public class CqlColumnFamilyDefinitionImpl implements ColumnFamilyDefinition {

	private Session session; 
	
	private String cfName; 
	private String keyspaceName;
	
	private Map<String, Object> properties = new HashMap<String, Object>();
	
	private List<ColumnDefinition> colDefList = new ArrayList<ColumnDefinition>();
	private List<ColumnDefinition> primaryKeyList = new ArrayList<ColumnDefinition>();
	
	private ByteBuffer keyAlias; 
	
	private boolean alterTable = false;
	
	private boolean initedViaResultSet = false; 

	public CqlColumnFamilyDefinitionImpl(Session session) {
		this.session = session;
	}

	
	public CqlColumnFamilyDefinitionImpl(Session session, String keyspace, Properties props) {
		this(session, keyspace, propertiesToMap(props));
	}

	public CqlColumnFamilyDefinitionImpl(Session session, String keyspace, Map<String, Object> options) {
		this.session = session;
		
		if (options == null) {
			options = new HashMap<String, Object>();
		}
		
		if (!options.containsKey("keyspace")) {
			options.put("keyspace", keyspace);
		}
		
		initFromMap(options);
	}
	

	public CqlColumnFamilyDefinitionImpl(Session session, String ksName, String cfName) {

		Query query = QueryBuilder.select().all()
				.from("system", "schema_columnfamilies")
				.where(eq("keyspace_name", ksName))
				.and(eq("columnfamily_name", cfName));

		ResultSet rs = session.execute(query);
		initFromResultSet(session, rs.one());
	}
	
	public CqlColumnFamilyDefinitionImpl(Session session, Row row) {
		initFromResultSet(session, row);
	}

	public CqlColumnFamilyDefinitionImpl(Session session, String keyspace, ColumnFamily<?, ?> columnFamily, Map<String, Object> options) {
		this.session = session;
		
		Preconditions.checkArgument(columnFamily != null, "ColumnFamily cannot be null");

		if (options == null) {
			options = new HashMap<String, Object>();
		}
		
		if (!options.containsKey("keyspace")) {
			options.put("keyspace", keyspace);
		}
		
		if (!options.containsKey("name")) {
			options.put("name", columnFamily.getName());
		}
		if (!options.containsKey("key_validation_class")) {
			options.put("key_validation_class", columnFamily.getKeySerializer().getComparatorType().getClassName());
		}
		if (!options.containsKey("comparator_type")) {
			options.put("comparator_type", columnFamily.getColumnSerializer().getComparatorType().getClassName());
		}
		if (!options.containsKey("default_validation_class")) {
			options.put("default_validation_class", columnFamily.getDefaultValueSerializer().getComparatorType().getClassName());
		}
		
		initFromMap(options);
	}
	
	
	private void initFromMap(Map<String, Object> options) {
		
		cfName = (String) options.remove("name");
		keyspaceName = (String) options.remove("keyspace");
		
		this.properties.putAll(options);

		String keyClass = (String) properties.remove("key_validation_class");
		keyClass = (keyClass == null) ?	keyClass = "blob" : keyClass;
		
		String comparatorClass = (String) properties.remove("comparator_type");
		comparatorClass = (comparatorClass == null) ?	comparatorClass = "blob" : comparatorClass;
		
		String dataValidationClass = (String) properties.remove("default_validation_class");
		dataValidationClass = (dataValidationClass == null) ?	dataValidationClass = "blob" : dataValidationClass;

		if (CqlFamilyFactory.OldStyleThriftMode()) {
			
			ColumnDefinition key = new CqlColumnDefinitionImpl().setName("key").setValidationClass(keyClass);
			primaryKeyList.add(key);
			ColumnDefinition column1 = new CqlColumnDefinitionImpl().setName("column1").setValidationClass(comparatorClass);
			primaryKeyList.add(column1);

			this.makeColumnDefinition().setName("value").setValidationClass(dataValidationClass);

		} else {
			throw new NotImplementedException();
		}
	}
	
	private void initFromResultSet(Session session, Row row) {

		if (row == null) {
			throw new RuntimeException("Result Set is empty");
		}

		this.session = session;
		initedViaResultSet = true; 

		this.setName(row.getString("columnfamily_name"));
		this.keyspaceName = row.getString("keyspace_name");

		properties.put("keyspace_name", row.getString("keyspace_name")); 
		properties.put("columnfamily_name", row.getString("columnfamily_name")); 
		properties.put("bloom_filter_fp_chance", row.getDouble("bloom_filter_fp_chance")); 
		properties.put("caching", row.getString("caching")); 
		properties.put("column_aliases", row.getString("column_aliases")); 
		properties.put("comment", row.getString("comment")); 
		properties.put("compaction_strategy_class", row.getString("compaction_strategy_class")); 
		properties.put("compaction_strategy_options", row.getString("compaction_strategy_options")); 
		properties.put("comparator", row.getString("comparator")); 
		properties.put("compression_parameters", row.getString("compression_parameters")); 
		properties.put("default_read_consistency", row.getString("default_read_consistency")); 
		properties.put("default_validator", row.getString("default_validator")); 
		properties.put("default_write_consistency", row.getString("default_write_consistency")); 
		properties.put("gc_grace_seconds", row.getInt("gc_grace_seconds")); 
		properties.put("id", row.getInt("id")); 
		properties.put("key_alias", row.getString("key_alias")); 
		properties.put("key_aliases", row.getString("key_aliases")); 
		properties.put("key_validator", row.getString("key_validator")); 
		properties.put("local_read_repair_chance", row.getDouble("local_read_repair_chance")); 
		properties.put("max_compaction_threshold", row.getInt("max_compaction_threshold")); 
		properties.put("min_compaction_threshold", row.getInt("min_compaction_threshold")); 
		properties.put("populate_io_cache_on_flush", row.getBool("populate_io_cache_on_flush")); 
		properties.put("read_repair_chance", row.getDouble("read_repair_chance")); 
		properties.put("replicate_on_write", row.getBool("replicate_on_write")); 
		properties.put("subcomparator", row.getString("subcomparator")); 
		properties.put("type", row.getString("type")); 
		properties.put("value_alias", row.getString("value_alias")); 

	}
    

	
	public CqlColumnFamilyDefinitionImpl alterTable() {
		alterTable = true;
		return this;
	}

	@Override
	public ColumnFamilyDefinition setComment(String comment) {
		properties.put("comment", "'" + comment + "'");
		return this;
	}

	@Override
	public String getComment() {
		return (String) properties.get("comment");
	}

	@Override
	public ColumnFamilyDefinition setKeyspace(String keyspace) {
		keyspaceName = keyspace;
		return this;
	}

	@Override
	public String getKeyspace() {
		return keyspaceName;
	}

	@Override
	@Deprecated
	public ColumnFamilyDefinition setMemtableFlushAfterMins(Integer value) {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public Integer getMemtableFlushAfterMins() {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public ColumnFamilyDefinition setMemtableOperationsInMillions(Double value) {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public Double getMemtableOperationsInMillions() {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public ColumnFamilyDefinition setMemtableThroughputInMb(Integer value) {
		throw new NotImplementedException();
	}

	@Override
	@Deprecated
	public Integer getMemtableThroughputInMb() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setMergeShardsChance(Double value) {
		throw new NotImplementedException();
	}

	@Override
	public Double getMergeShardsChance() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setMinCompactionThreshold(Integer value) {
		properties.put("min_compaction_threshold", value);
		return this;
	}

	@Override
	public Integer getMinCompactionThreshold() {
		return (Integer) properties.get("min_compaction_threshold");
	}

	@Override
	public ColumnFamilyDefinition setMaxCompactionThreshold(Integer value) {
		properties.put("max_compaction_threshold", value);
		return this;
	}

	@Override
	public Integer getMaxCompactionThreshold() {
		return (Integer) properties.get("max_compaction_threshold");
	}

	@Override
	public ColumnFamilyDefinition setCompactionStrategy(String strategy) {
		properties.put("compaction_strategy_class", strategy);
		return this;
	}

	@Override
	public String getCompactionStrategy() {
		return (String) properties.get("compaction_strategy_class");
	}

	@Override
	public ColumnFamilyDefinition setCompactionStrategyOptions(Map<String, String> options) {
		properties.put("compaction_strategy_options", toJsonString(options));
		return this;
	}

	@Override
	public Map<String, String> getCompactionStrategyOptions() {
		return fromJsonString((String) properties.get("compaction_strategy_options"));
	}

	@Override
	public ColumnFamilyDefinition setCompressionOptions(Map<String, String> options) {
		properties.put("compression_parameters", toJsonString(options));
		return this;
	}

	@Override
	public Map<String, String> getCompressionOptions() {
		return fromJsonString((String) properties.get("compression_parameters"));
	}

	
	@Override
	public ColumnFamilyDefinition setBloomFilterFpChance(Double chance) {
		properties.put("bloom_filter_fp_chance", chance);
		return this;
	}

	@Override
	public Double getBloomFilterFpChance() {
		return (Double) properties.get("bloom_filter_fp_chance");
	}

	@Override
	public ColumnFamilyDefinition setCaching(String caching) {
		properties.put("caching", caching);
		return this;
	}

	@Override
	public String getCaching() {
		return (String) properties.get("caching");
	}

	@Override
	public ColumnFamilyDefinition setName(String name) {
		cfName = name;
		return this;
	}

	@Override
	public String getName() {
		return cfName;
	}

	@Override
	public ColumnFamilyDefinition setReadRepairChance(Double value) {
		properties.put("read_repair_chance", value);
		return this;
	}

	@Override
	public Double getReadRepairChance() {
		return (Double) properties.get("read_repair_chance");
	}

	@Override
	public ColumnFamilyDefinition setLocalReadRepairChance(Double value) {
		properties.put("local_read_repair_chance", value);
		return this;
	}

	@Override
	public Double getLocalReadRepairChance() {
		return (Double) properties.get("local_read_repair_chance");
	}

	@Override
	public ColumnFamilyDefinition setReplicateOnWrite(Boolean value) {
		properties.put("replicate_on_write", value);
		return this;
	}

	@Override
	public Boolean getReplicateOnWrite() {
		return (Boolean) properties.get("replicate_on_write");
	}

	@Override
	public ColumnFamilyDefinition setRowCacheProvider(String value) {
		throw new NotImplementedException();
	}

	@Override
	public String getRowCacheProvider() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(Integer value) {
		throw new NotImplementedException();
	}

	@Override
	public Integer getRowCacheSavePeriodInSeconds() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setRowCacheSize(Double size) {
		throw new NotImplementedException();
	}

	@Override
	public Double getRowCacheSize() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setComparatorType(String value) {
		properties.put("comparator", value);
		return this;
	}

	@Override
	public String getComparatorType() {
		return (String) properties.get("comparator");
	}

	@Override
	public ColumnFamilyDefinition setDefaultValidationClass(String value) {
		throw new NotImplementedException();
	}

	@Override
	public String getDefaultValidationClass() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setId(Integer id) {
		properties.put("id", id);
		return this;
	}

	@Override
	public Integer getId() {
		return (Integer) properties.get("id");
	}

	@Override
	public ColumnFamilyDefinition setKeyAlias(ByteBuffer alias) {

		keyAlias = alias;
		setKeyAlias(CQL3Type.Native.TEXT.getType().getString(alias));
		
		return this;
	}
	
	private void setKeyAlias(String keyAlias) {
		// TODO: won't work for composite columns, fix this!
		ColumnDefinition primaryKeyCol; 

		if (primaryKeyList.size() > 0) {
			primaryKeyCol = primaryKeyList.get(0); 
		} else {
			primaryKeyCol = new CqlColumnDefinitionImpl();
			primaryKeyList.add(primaryKeyCol);
		}
				
		primaryKeyCol.setName(keyAlias);
	}

	@Override
	public ByteBuffer getKeyAlias() {
		return keyAlias;
	}

	@Override
	public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(Integer value) {
		throw new NotImplementedException();
	}

	@Override
	public Integer getKeyCacheSavePeriodInSeconds() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setKeyCacheSize(Double keyCacheSize) {
		throw new NotImplementedException();
	}

	@Override
	public Double getKeyCacheSize() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass) {
		// nothing to do here.
		properties.put("key_validation_class", keyValidationClass);
		return this;
	}
	
	private void addToPartitionKey(String columnName, String validationClass) {
		ColumnDefinition partitionKeyCol = new CqlColumnDefinitionImpl().setName(columnName);
		primaryKeyList.add(partitionKeyCol);
	}

	@Override
	public String getKeyValidationClass() {
		// TOOD: won't work for composite columns, fix this!
		if (primaryKeyList.size() > 0) {
			return primaryKeyList.get(0).getValidationClass();
		} else {
			return null;
		}
	}

	@Override
	public List<ColumnDefinition> getColumnDefinitionList() {
		if (colDefList.isEmpty() && initedViaResultSet) {
			
			Query query = QueryBuilder.select().from("system", "schema_columns")
			.where(eq("keyspace_name", keyspaceName))
			.and(eq("columnfamily_name", cfName));
			
			ResultSet rs = session.execute(query);
			for (Row row : rs.all()) {
				colDefList.add(new CqlColumnDefinitionImpl(row));
			}
		}
		return colDefList;
	}

	@Override
	public ColumnFamilyDefinition addColumnDefinition(ColumnDefinition def) {
		colDefList.add(def);
		return this;
	}

	@Override
	public ColumnDefinition makeColumnDefinition() {
		ColumnDefinition colDef = new CqlColumnDefinitionImpl();
		colDefList.add(colDef);
		return colDef;
	}

	@Override
	public void clearColumnDefinitionList() {
		colDefList.clear();
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
	public ColumnFamilyDefinition setFieldValue(String name, Object value) {
		throw new NotImplementedException();
	}

	@Override
	public ColumnFamilyDefinition setGcGraceSeconds(Integer seconds) {
		properties.put("gc_grace_seconds", seconds);
		return this;
	}

	@Override
	public Integer getGcGraceSeconds() {
		return (Integer) properties.get("gc_grace_seconds");
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
	public Properties getProperties() {
		Properties props = new Properties();
		for (String key : properties.keySet()) {
			if (properties.get(key) != null) {
				props.put(key, properties.get(key));
			}
		}
		return props;
	}

	@Override
	public void setProperties(Properties additionalProperties) throws Exception {
		
		Map<String, Object> props = propertiesToMap(additionalProperties);
		properties.putAll(props);
	}

	public OperationResult<SchemaChangeResult> execute() {
		
		String query = (alterTable) ? getUpdateQuery() : getCreateQuery();
		System.out.println("Query: " + query);
		
		ResultSet rs = session.execute(query);

		return new CqlOperationResultImpl<SchemaChangeResult>(rs, null);
	}
	
	private String getCreateQuery() {
		StringBuilder sb = new StringBuilder("CREATE TABLE ");
		sb.append(keyspaceName).append(".").append(cfName);
		sb.append(" ( ");
		
		boolean compositePrimaryKey = primaryKeyList.size() > 1;
		
		if (!compositePrimaryKey) {
			
			appendColDefinition(sb, primaryKeyList.iterator());
			sb.append(" PRIMARY KEY, ");
			appendColDefinition(sb, colDefList.iterator());
			
		} else {
			appendColDefinition(sb, primaryKeyList.iterator());
			sb.append(" ,");
			appendColDefinition(sb, colDefList.iterator());
			sb.append(", PRIMARY KEY (");
			appendPrimaryKeyDefinition(sb, primaryKeyList.iterator());
			sb.append(") ");
		}
		
		sb.append(")");
		
		if (properties.size() > 0) {
			sb.append(" WITH ");
			
			Iterator<String> propIter = properties.keySet().iterator();
			while(propIter.hasNext()) {
				
				String pKey = propIter.next();
				Object pValue = properties.get(pKey);
				
				if (pValue == null) {
					continue;
				}
				
				if (pValue instanceof String) {
					sb.append(pKey).append(" = '").append(pValue).append("'");
				} else {
					sb.append(pKey).append(" = ").append(pValue);
				}
				
								
				if (propIter.hasNext()) {
					sb.append(" AND ");
				}
			}
		}
		
		String query = sb.toString();

		return query;
	}

	private String getUpdateQuery() {
		
		StringBuilder sb = new StringBuilder("ALTER TABLE ");
		sb.append(keyspaceName).append(".").append(cfName);
		
			sb.append(" WITH ");
			
			Iterator<String> propIter = properties.keySet().iterator();
			while(propIter.hasNext()) {
				
				String pKey = propIter.next();
				Object pValue = properties.get(pKey);
				
				sb.append(pKey).append(" = ").append(pValue);
				
				if (propIter.hasNext()) {
					sb.append(" AND ");
				}
			}
		return sb.toString();
	}

	private void appendColDefinition(StringBuilder sb, Iterator<ColumnDefinition> iter) {
		
		while (iter.hasNext()) {
			CqlColumnDefinitionImpl colDef = (CqlColumnDefinitionImpl) iter.next(); 
			sb.append(colDef.getName()).append(" ").append(colDef.getCqlType());
			if (iter.hasNext()) {
				sb.append(", ");
			}
		}
	}

	private void appendPrimaryKeyDefinition(StringBuilder sb, Iterator<ColumnDefinition> iter) {
		
		while (iter.hasNext()) {
			CqlColumnDefinitionImpl colDef = (CqlColumnDefinitionImpl) iter.next(); 
			sb.append(colDef.getName());
			if (iter.hasNext()) {
				sb.append(", ");
			}
		}
	}
	
	private static Map<String, Object> propertiesToMap(Properties props) {
		Map<String, Object> root = Maps.newTreeMap();
		if (props == null) {
			return root;
		}
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
	
	private static String toJsonString(Map<String, String> options) {
		if (options == null) {
			return null;
		}
		
		JSONObject json = new JSONObject();
		for(String key : options.keySet()) {
			try {
				json.put(key, options.get(key));
			} catch (JSONException e) {
				throw new RuntimeException(e);
			}
		}
		return json.toString();
	}
	
	private static Map<String, String> fromJsonString(String jsonString) {
		if (jsonString == null) {
			return new HashMap<String, String>();
		}
		try {
			JSONObject json = new JSONObject(jsonString);
			Map<String, String> map = new HashMap<String, String>();
			Iterator<String> iter = json.keys();
			while(iter.hasNext()) {
				String key = iter.next();
				String value = json.getString(key).toString();
				map.put(key, value);
			}
			return map;
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
	private void checkEmptyOptions() {
		if (properties.size() == 0) {
			// add a comment by default
			properties.put("comment", "default");
		}
	}

}
