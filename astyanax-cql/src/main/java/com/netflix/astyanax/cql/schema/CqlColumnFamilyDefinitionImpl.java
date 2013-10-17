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
	
	private Map<String, Object> optionsMap = new HashMap<String, Object>();
	
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
		this.keyspaceName = keyspace;
		
		if (options == null) {
			options = new HashMap<String, Object>();
		}
		initFromMap(options);
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
		
		keyspaceName = keyspace;
		cfName = columnFamily.getName();

		optionsMap.put("key_validation_class", columnFamily.getKeySerializer().getComparatorType().getClassName());
		optionsMap.put("comparator_type", columnFamily.getColumnSerializer().getComparatorType().getClassName());
		optionsMap.put("default_validation_class", columnFamily.getDefaultValueSerializer().getComparatorType().getClassName());
		
		initFromMap(options);
	}
	
	
	private void initFromMap(Map<String, Object> options) {
		
		String cName = (String) options.get("name");
		if (cName != null) {
			cfName = cName;
			options.remove("name");
		}
		
		String kName = (String) options.get("keyspace");
		if (kName != null) {
			keyspaceName = kName;
		}
		
		this.optionsMap.putAll(options);

		String keyClass = (String) optionsMap.remove("key_validation_class");
		keyClass = (keyClass == null) ?	keyClass = "blob" : keyClass;
		
		String comparatorClass = (String) optionsMap.remove("comparator_type");
		comparatorClass = (comparatorClass == null) ?	comparatorClass = "blob" : comparatorClass;
		
		String dataValidationClass = (String) optionsMap.remove("default_validation_class");
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

		this.keyspaceName = row.getString("keyspace_name");
		this.cfName = row.getString("columnfamily_name");

		optionsMap.put("keyspace_name", row.getString("keyspace_name")); 
		optionsMap.put("columnfamily_name", row.getString("columnfamily_name")); 
		optionsMap.put("bloom_filter_fp_chance", row.getDouble("bloom_filter_fp_chance")); 
		optionsMap.put("caching", row.getString("caching")); 
		optionsMap.put("column_aliases", row.getString("column_aliases")); 
		optionsMap.put("comment", row.getString("comment")); 
		optionsMap.put("compaction_strategy_class", row.getString("compaction_strategy_class")); 
		optionsMap.put("compaction_strategy_options", row.getString("compaction_strategy_options")); 
		optionsMap.put("comparator", row.getString("comparator")); 
		optionsMap.put("compression_parameters", row.getString("compression_parameters")); 
		optionsMap.put("default_read_consistency", row.getString("default_read_consistency")); 
		optionsMap.put("default_validator", row.getString("default_validator")); 
		optionsMap.put("default_write_consistency", row.getString("default_write_consistency")); 
		optionsMap.put("gc_grace_seconds", row.getInt("gc_grace_seconds")); 
		optionsMap.put("id", row.getInt("id")); 
		optionsMap.put("key_alias", row.getString("key_alias")); 
		optionsMap.put("key_aliases", row.getString("key_aliases")); 
		optionsMap.put("key_validator", row.getString("key_validator")); 
		optionsMap.put("local_read_repair_chance", row.getDouble("local_read_repair_chance")); 
		optionsMap.put("max_compaction_threshold", row.getInt("max_compaction_threshold")); 
		optionsMap.put("min_compaction_threshold", row.getInt("min_compaction_threshold")); 
		optionsMap.put("populate_io_cache_on_flush", row.getBool("populate_io_cache_on_flush")); 
		optionsMap.put("read_repair_chance", row.getDouble("read_repair_chance")); 
		optionsMap.put("replicate_on_write", row.getBool("replicate_on_write")); 
		optionsMap.put("subcomparator", row.getString("subcomparator")); 
		optionsMap.put("type", row.getString("type")); 
		optionsMap.put("value_alias", row.getString("value_alias")); 
	}
	
	public CqlColumnFamilyDefinitionImpl alterTable() {
		alterTable = true;
		return this;
	}

	@Override
	public ColumnFamilyDefinition setComment(String comment) {
		optionsMap.put("comment", "'" + comment + "'");
		return this;
	}

	@Override
	public String getComment() {
		return (String) optionsMap.get("comment");
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
		optionsMap.put("min_compaction_threshold", value);
		return this;
	}

	@Override
	public Integer getMinCompactionThreshold() {
		return (Integer) optionsMap.get("min_compaction_threshold");
	}

	@Override
	public ColumnFamilyDefinition setMaxCompactionThreshold(Integer value) {
		optionsMap.put("max_compaction_threshold", value);
		return this;
	}

	@Override
	public Integer getMaxCompactionThreshold() {
		return (Integer) optionsMap.get("max_compaction_threshold");
	}

	@Override
	public ColumnFamilyDefinition setCompactionStrategy(String strategy) {
		optionsMap.put("compaction_strategy_class", strategy);
		return this;
	}

	@Override
	public String getCompactionStrategy() {
		return (String) optionsMap.get("compaction_strategy_class");
	}

	@Override
	public ColumnFamilyDefinition setCompactionStrategyOptions(Map<String, String> options) {
		optionsMap.put("compaction_strategy_options", toJsonString(options));
		return this;
	}

	@Override
	public Map<String, String> getCompactionStrategyOptions() {
		return fromJsonString((String) optionsMap.get("compaction_strategy_options"));
	}

	@Override
	public ColumnFamilyDefinition setCompressionOptions(Map<String, String> options) {
		optionsMap.put("compression_parameters", toJsonString(options));
		return this;
	}

	@Override
	public Map<String, String> getCompressionOptions() {
		return fromJsonString((String) optionsMap.get("compression_parameters"));
	}

	
	@Override
	public ColumnFamilyDefinition setBloomFilterFpChance(Double chance) {
		optionsMap.put("bloom_filter_fp_chance", chance);
		return this;
	}

	@Override
	public Double getBloomFilterFpChance() {
		return (Double) optionsMap.get("bloom_filter_fp_chance");
	}

	@Override
	public ColumnFamilyDefinition setCaching(String caching) {
		optionsMap.put("caching", caching);
		return this;
	}

	@Override
	public String getCaching() {
		return (String) optionsMap.get("caching");
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
		optionsMap.put("read_repair_chance", value);
		return this;
	}

	@Override
	public Double getReadRepairChance() {
		return (Double) optionsMap.get("read_repair_chance");
	}

	@Override
	public ColumnFamilyDefinition setLocalReadRepairChance(Double value) {
		optionsMap.put("local_read_repair_chance", value);
		return this;
	}

	@Override
	public Double getLocalReadRepairChance() {
		return (Double) optionsMap.get("local_read_repair_chance");
	}

	@Override
	public ColumnFamilyDefinition setReplicateOnWrite(Boolean value) {
		optionsMap.put("replicate_on_write", value);
		return this;
	}

	@Override
	public Boolean getReplicateOnWrite() {
		return (Boolean) optionsMap.get("replicate_on_write");
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
		optionsMap.put("comparator", value);
		return this;
	}

	@Override
	public String getComparatorType() {
		return (String) optionsMap.get("comparator");
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
		optionsMap.put("id", id);
		return this;
	}

	@Override
	public Integer getId() {
		return (Integer) optionsMap.get("id");
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
		optionsMap.put("key_validation_class", keyValidationClass);
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
		optionsMap.put("gc_grace_seconds", seconds);
		return this;
	}

	@Override
	public Integer getGcGraceSeconds() {
		return (Integer) optionsMap.get("gc_grace_seconds");
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
		for (String key : optionsMap.keySet()) {
			if (optionsMap.get(key) != null) {
				props.put(key, optionsMap.get(key));
			}
		}
		return props;
	}

	@Override
	public void setProperties(Properties additionalProperties) throws Exception {
		
		Map<String, Object> props = propertiesToMap(additionalProperties);
		optionsMap.putAll(props);
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
		
		if (optionsMap.size() > 0) {
			sb.append(" WITH ");
			
			Iterator<String> propIter = optionsMap.keySet().iterator();
			while(propIter.hasNext()) {
				
				String pKey = propIter.next();
				Object pValue = optionsMap.get(pKey);
				
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
			
			Iterator<String> propIter = optionsMap.keySet().iterator();
			while(propIter.hasNext()) {
				
				String pKey = propIter.next();
				Object pValue = optionsMap.get(pKey);
				
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
		if (optionsMap.size() == 0) {
			// add a comment by default
			optionsMap.put("comment", "default");
		}
	}

}
