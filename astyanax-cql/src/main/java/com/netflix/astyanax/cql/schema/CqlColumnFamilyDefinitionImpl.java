package com.netflix.astyanax.cql.schema;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.reads.CFRowQueryGen;
import com.netflix.astyanax.cql.util.DataTypeMapping;
import com.netflix.astyanax.cql.writes.CFMutationQueryGen;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

/**
 * Impl for {@link ColumnFamilyDefinition} interface that constructs it's state from the java driver {@link ResultSet}
 * 
 * @author poberai
 *
 */
public class CqlColumnFamilyDefinitionImpl implements ColumnFamilyDefinition {

	private Session session; 
	
	private String cfName; 
	private String keyspaceName;
	
	private Map<String, Object> optionsMap = new HashMap<String, Object>();
	
	private List<ColumnDefinition> partitionKeyList = new ArrayList<ColumnDefinition>();
	private List<ColumnDefinition> clusteringKeyList = new ArrayList<ColumnDefinition>();
	private List<ColumnDefinition> regularColumnList = new ArrayList<ColumnDefinition>();
	private List<ColumnDefinition> allColumnsDefinitionList = new ArrayList<ColumnDefinition>();
	
	private String[] allPkColNames;
	
	private AnnotatedCompositeSerializer<?> compositeSerializer = null; 
	
	private boolean alterTable = false;

	private CFMutationQueryGen mutationQueryGen = null;
	private CFRowQueryGen rowQueryGen = null;
	
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
		mutationQueryGen = new CFMutationQueryGen(session, keyspaceName, this);
		rowQueryGen = new CFRowQueryGen(session, keyspaceName, this);
	}

	public CqlColumnFamilyDefinitionImpl(Session session, String keyspace, ColumnFamily<?, ?> columnFamily, Map<String, Object> options) {
		this.session = session;
		
		Preconditions.checkArgument(columnFamily != null, "ColumnFamily cannot be null");

		if (options == null) {
			options = new HashMap<String, Object>();
		}
		
		keyspaceName = keyspace;
		cfName = columnFamily.getName();

		optionsMap.put("key_validator", columnFamily.getKeySerializer().getComparatorType().getClassName());
		optionsMap.put("comparator", columnFamily.getColumnSerializer().getComparatorType().getClassName());
		optionsMap.put("default_validator", columnFamily.getDefaultValueSerializer().getComparatorType().getClassName());
		
		if (columnFamily.getColumnSerializer() instanceof AnnotatedCompositeSerializer) {
			compositeSerializer = (AnnotatedCompositeSerializer<?>) columnFamily.getColumnSerializer();
		}
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
		
		if (optionsMap.containsKey("key_validation_class")) {
			optionsMap.put("key_validator", optionsMap.remove("key_validation_class"));
		}
		if (optionsMap.containsKey("comparator_type")) {
			optionsMap.put("comparator", optionsMap.remove("comparator_type"));
		}
		if (optionsMap.containsKey("default_validation_class")) {
			optionsMap.put("default_validator", optionsMap.remove("default_validation_class"));
		}
	}
	

	
	private void initFromResultSet(Session session, Row row) {

		if (row == null) {
			throw new RuntimeException("Result Set is empty");
		}

		this.session = session;

		this.keyspaceName = row.getString("keyspace_name");
		this.cfName = row.getString("columnfamily_name");

		List<Definition> colDefs = row.getColumnDefinitions().asList();
		 for (Definition colDef : colDefs) {
			 
			 String colName = colDef.getName();
			 DataType dataType = colDef.getType();
			 Object value = DataTypeMapping.getDynamicColumn(row, colName, dataType);
			 optionsMap.put(colName, value);
		 }
		readColDefinitions();
		
		/**
		if (partitionKeyList.size() == 0) {
			readColDefinitionsForCass12();
		}
		*/
	}

	private void processCompositeComparator() {

		int colIndex = 1;
		for (ComponentSerializer<?> componentSerializer : compositeSerializer.getComponents()) {
			String typeName = componentSerializer.getSerializer().getComparatorType().getTypeName();
			ColumnDefinition column = new CqlColumnDefinitionImpl().setName("column" + colIndex++).setValidationClass(typeName);
			clusteringKeyList.add(column);
		}
	}
	
	private void processCompositeComparatorSpec(String comparatorSpec) {
		// e.g  CompositeType(UTF8Type, LongType, UTF8Type)
		
		String regex = "[\\(,\\)]";
		Pattern pattern = Pattern.compile(regex);
		String[] parts = pattern.split(comparatorSpec);

		int colIndex = 1;
		for (int i=1; i<parts.length; i++) {
			String componentTypeString = parts[i].trim();
			ColumnDefinition column = new CqlColumnDefinitionImpl().setName("column" + colIndex++).setValidationClass(componentTypeString);
			clusteringKeyList.add(column);
		}
	}
	
	private void createColumnDefinitions() {

		String keyClass = (String) optionsMap.remove("key_validator");
		
		String defaultBytesType = ComparatorType.BYTESTYPE.getTypeName();
		
		keyClass = (keyClass == null) ?	keyClass = defaultBytesType : keyClass;
		
		String comparatorClass = (String) optionsMap.remove("comparator");
		comparatorClass = (comparatorClass == null) ?	comparatorClass = defaultBytesType : comparatorClass;
		
		String dataValidationClass = (String) optionsMap.remove("default_validator");
		dataValidationClass = (dataValidationClass == null) ?	dataValidationClass = defaultBytesType : dataValidationClass;

		ColumnDefinition key = new CqlColumnDefinitionImpl().setName("key").setValidationClass(keyClass);
		partitionKeyList.add(key);

		if (compositeSerializer != null) {
			processCompositeComparator();
		} else if (comparatorClass.contains("CompositeType")) {
			processCompositeComparatorSpec(comparatorClass);
		} else {
			ColumnDefinition column1 = new CqlColumnDefinitionImpl().setName("column1").setValidationClass(comparatorClass);
			clusteringKeyList.add(column1);
		}

		ColumnDefinition valueColumn = new CqlColumnDefinitionImpl().setName("value").setValidationClass(dataValidationClass);
		this.regularColumnList.add(valueColumn);
	}
	
	/**
	private void readColDefinitionsForCass12() {

		String defaultBytesType = ComparatorType.BYTESTYPE.getTypeName();

		String keyClass = (String) optionsMap.get("key_validator");
		keyClass = (keyClass == null) ?	keyClass = defaultBytesType : keyClass;
		
		String comparatorClass = (String) optionsMap.get("comparator");
		comparatorClass = (comparatorClass == null) ?	comparatorClass = defaultBytesType : comparatorClass;
		
		String dataValidationClass = (String) optionsMap.get("default_validator");
		dataValidationClass = (dataValidationClass == null) ?	dataValidationClass = defaultBytesType : dataValidationClass;

		ColumnDefinition key = new CqlColumnDefinitionImpl().setName("key").setValidationClass(keyClass);
		partitionKeyList.add(key);

		if (compositeSerializer != null) {
			processCompositeComparator();
		} else if (comparatorClass.contains("CompositeType")) {
			processCompositeComparatorSpec(comparatorClass);
		} else {
			ColumnDefinition column1 = new CqlColumnDefinitionImpl().setName("column1").setValidationClass(comparatorClass);
			clusteringKeyList.add(column1);
		}

		ColumnDefinition valueColumn = new CqlColumnDefinitionImpl().setName("value").setValidationClass(dataValidationClass);
		this.regularColumnList.add(valueColumn);
		
		this.allColumnsDefinitionList.addAll(partitionKeyList);
		this.allColumnsDefinitionList.addAll(clusteringKeyList);
		this.allColumnsDefinitionList.addAll(regularColumnList);
		
		List<String> allPrimaryKeyColNames = new ArrayList<String>();
		for (ColumnDefinition colDef : partitionKeyList) {
			allPrimaryKeyColNames.add(colDef.getName());
		}
		for (ColumnDefinition colDef : clusteringKeyList) {
			allPrimaryKeyColNames.add(colDef.getName());
		}
		
		allPkColNames = allPrimaryKeyColNames.toArray(new String[allPrimaryKeyColNames.size()]);
	}
	*/
	
	private void readColDefinitions() {
		
		// VALUE COLUMNS AND COLUMNS THAT ARE NOT PART OF THE PRIMARY KEY
		Statement query = QueryBuilder.select().from("system", "schema_columns")
				.where(eq("keyspace_name", keyspaceName))
				.and(eq("columnfamily_name", cfName));
		
		ResultSet rs = session.execute(query);
		List<Row> rows = rs.all();
		if (rows != null && rows.size() > 0) {
			
			List<CqlColumnDefinitionImpl> tmpList = new ArrayList<CqlColumnDefinitionImpl>();
			
			for (Row row : rows) {
				CqlColumnDefinitionImpl colDef = new CqlColumnDefinitionImpl(row);
				switch (colDef.getColumnType()) {
				case partition_key:
					partitionKeyList.add(colDef);
					allColumnsDefinitionList.add(colDef);
					break;
				case clustering_key:
					tmpList.add(colDef);
					allColumnsDefinitionList.add(colDef);
					break;
				case regular:
					regularColumnList.add(colDef);
					allColumnsDefinitionList.add(colDef);
					break;
				case compact_value:
					regularColumnList.add(colDef);
					allColumnsDefinitionList.add(colDef);
					break;
				}
			}

			Collections.sort(tmpList);
			clusteringKeyList.addAll(tmpList);
			tmpList = null;
			
			List<String> allPrimaryKeyColNames = new ArrayList<String>();
			for (ColumnDefinition colDef : partitionKeyList) {
				allPrimaryKeyColNames.add(colDef.getName());
			}
			for (ColumnDefinition colDef : clusteringKeyList) {
				allPrimaryKeyColNames.add(colDef.getName());
			}
			
			allPkColNames = allPrimaryKeyColNames.toArray(new String[allPrimaryKeyColNames.size()]);
		}
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
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public Integer getMemtableFlushAfterMins() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public ColumnFamilyDefinition setMemtableOperationsInMillions(Double value) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public Double getMemtableOperationsInMillions() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public ColumnFamilyDefinition setMemtableThroughputInMb(Integer value) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	@Deprecated
	public Integer getMemtableThroughputInMb() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyDefinition setMergeShardsChance(Double value) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Double getMergeShardsChance() {
		throw new UnsupportedOperationException("Operation not supported");
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
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public String getRowCacheProvider() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyDefinition setRowCacheSavePeriodInSeconds(Integer value) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Integer getRowCacheSavePeriodInSeconds() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyDefinition setRowCacheSize(Double size) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Double getRowCacheSize() {
		throw new UnsupportedOperationException("Operation not supported");
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
		optionsMap.put("default_validator", value);
		return this;
	}

	@Override
	public String getDefaultValidationClass() {
		return (String) optionsMap.get("default_validator");
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
		throw new UnsupportedOperationException("Operation not supported");
	}
	
	@Override
	public ByteBuffer getKeyAlias() {
		return null;
	}

	@Override
	public ColumnFamilyDefinition setKeyCacheSavePeriodInSeconds(Integer value) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Integer getKeyCacheSavePeriodInSeconds() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyDefinition setKeyCacheSize(Double keyCacheSize) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Double getKeyCacheSize() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnFamilyDefinition setKeyValidationClass(String keyValidationClass) {
		optionsMap.put("key_validator", keyValidationClass);
		return this;
	}
	
	@Override
	public String getKeyValidationClass() {
		return (String) optionsMap.get("key_validator");
	}

	public ColumnDefinition getPartitionKeyColumnDefinition() {
		return partitionKeyList.get(0);
	}

	public List<ColumnDefinition> getRegularColumnDefinitionList() {
		return regularColumnList;
	}

	public List<ColumnDefinition> getPartitionKeyColumnDefinitionList() {
		return partitionKeyList;
	}
	
	public List<ColumnDefinition> getClusteringKeyColumnDefinitionList() {
		return clusteringKeyList;
	}

	public String[] getAllPkColNames() {
		return allPkColNames;
	}

	@Override
	public ColumnFamilyDefinition addColumnDefinition(ColumnDefinition def) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public ColumnDefinition makeColumnDefinition() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public void clearColumnDefinitionList() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public Collection<String> getFieldNames() {
		return optionsMap.keySet();
	}

	@Override
	public Object getFieldValue(String name) {
		return optionsMap.get(name);
	}

	@Override
	public ColumnFamilyDefinition setFieldValue(String name, Object value) {
		optionsMap.put(name, value);
		return this;
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
		List<FieldMetadata> list = new ArrayList<FieldMetadata>();
		
		for (String key : optionsMap.keySet()) {
			Object value = optionsMap.get(key);
			
			Class<?> clazz = value.getClass();
			
			String name = key.toUpperCase();
			String type = clazz.getSimpleName().toUpperCase();
			boolean isContainer = Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz);
			list.add(new FieldMetadata(name, type, isContainer));
		}
		return list;
	}

	@Override
	public void setFields(Map<String, Object> options) {
		optionsMap.putAll(options);
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
		
		createColumnDefinitions();
		
		String query = (alterTable) ? getUpdateQuery() : getCreateQuery();
		ResultSet rs = session.execute(query);

		return new CqlOperationResultImpl<SchemaChangeResult>(rs, null);
	}
	
	private String getCreateQuery() {
		StringBuilder sb = new StringBuilder("CREATE TABLE ");
		sb.append(keyspaceName).append(".").append(cfName);
		sb.append(" ( ");
		
		boolean compositePrimaryKey = clusteringKeyList.size() > 0;
		
		if (!compositePrimaryKey) {
			
			appendColDefinition(sb, partitionKeyList.iterator());
			sb.append(" PRIMARY KEY, ");
			appendColDefinition(sb, regularColumnList.iterator());
			
		} else {
			appendColDefinition(sb, partitionKeyList.iterator());
			sb.append(" ,");
			appendColDefinition(sb, clusteringKeyList.iterator());
			sb.append(" ,");
			appendColDefinition(sb, regularColumnList.iterator());
			sb.append(", PRIMARY KEY (");
			appendPrimaryKeyDefinition(sb, partitionKeyList.iterator(), clusteringKeyList.iterator());
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

	private void appendPrimaryKeyDefinition(StringBuilder sb, Iterator<ColumnDefinition> iter1, Iterator<ColumnDefinition> iter2) {
		
		while (iter1.hasNext()) {
			CqlColumnDefinitionImpl colDef = (CqlColumnDefinitionImpl) iter1.next(); 
			sb.append(colDef.getName());
			if (iter1.hasNext()) {
				sb.append(", ");
			}
		}
		if (iter2.hasNext()) {
			sb.append(", ");

			while (iter2.hasNext()) {
				CqlColumnDefinitionImpl colDef = (CqlColumnDefinitionImpl) iter2.next(); 
				sb.append(colDef.getName());
				if (iter2.hasNext()) {
					sb.append(", ");
				}
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

	@Override
	public List<ColumnDefinition> getColumnDefinitionList() {
		return allColumnsDefinitionList;
	}
	
	public CFMutationQueryGen getMutationQueryGenerator() {
		return mutationQueryGen;
	}
	
	public CFRowQueryGen getRowQueryGenerator() {
		return rowQueryGen;
	}
	
	public void printOptionsMap() {
		for (String key : optionsMap.keySet()) {
			System.out.println(key + " " +  optionsMap.get(key));
		}
	}
}
