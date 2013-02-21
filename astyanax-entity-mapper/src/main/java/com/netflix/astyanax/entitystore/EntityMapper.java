package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import java.util.ArrayList;

/**
 * utility class to map btw root Entity and cassandra data model
 * @param <T> entity type 
 * @param <K> rowKey type
 */
class EntityMapper<T, K> {

	private final Class<T> clazz;
	private final Integer ttl;
	private final Field idField;
	private final Map<String, ColumnMapper> columnList;
	private final String entityName;
	
	/**
	 * 
	 * @param clazz
	 * @throws IllegalArgumentException 
	 * 		if clazz is NOT annotated with @Entity
	 * 		if column name contains illegal char (like dot)
	 */
	EntityMapper(Class<T> clazz, Integer ttl) {
		this.clazz = clazz;
		
		// clazz should be annotated with @Entity
		Entity entityAnnotation = clazz.getAnnotation(Entity.class);
		if(entityAnnotation == null)
			throw new IllegalArgumentException("class is NOT annotated with @java.persistence.Entity: " + clazz.getName());
		
		entityName = MappingUtils.getEntityName(entityAnnotation, clazz);
		
		if(ttl == null) {
			// try @TTL annotation at entity/row level.
			// it doesn't make sense to support @TTL annotation at individual column level.
			TTL ttlAnnotation = clazz.getAnnotation(TTL.class);
			if(ttlAnnotation == null)
				this.ttl = null;
			else
				this.ttl = ttlAnnotation.value();
		} else {
			this.ttl = ttl;
		}

		Field[] declaredFields = getFields(clazz, true).toArray(new Field[0]);
		columnList = Maps.newHashMapWithExpectedSize(declaredFields.length);
		Set<String> usedColumnNames = Sets.newHashSet();
		Field tmpIdField = null;
		for (Field field : declaredFields) {
			Id idAnnotation = field.getAnnotation(Id.class);
			if(idAnnotation != null) {
				Preconditions.checkArgument(tmpIdField == null, "there are multiple fields with @Id annotation");
				field.setAccessible(true);
				tmpIdField = field;
			}
			Column columnAnnotation = field.getAnnotation(Column.class);
			if ((columnAnnotation != null)) {
				field.setAccessible(true);
				ColumnMapper columnMapper = null;
				Entity compositeAnnotation = field.getType().getAnnotation(Entity.class);
			    if (Map.class.isAssignableFrom(field.getType())) {
                    columnMapper = new MapColumnMapper(field);
                } else if (Set.class.isAssignableFrom(field.getType())) {
                    columnMapper = new SetColumnMapper(field);
                } else if(compositeAnnotation == null) {
	                columnMapper = new LeafColumnMapper(field);
				} else {
	                columnMapper = new CompositeColumnMapper(field);
				}
				Preconditions.checkArgument(!usedColumnNames.contains(columnMapper.getColumnName()), 
						String.format("duplicate case-insensitive column name: %s", columnMapper.getColumnName().toLowerCase()));
				columnList.put(columnMapper.getColumnName(), columnMapper);
				usedColumnNames.add(columnMapper.getColumnName().toLowerCase());
			}
		}
		Preconditions.checkNotNull(tmpIdField, "there are no field with @Id annotation");
		//Preconditions.checkArgument(tmpIdField.getClass().equals(K.getClass()), String.format("@Id field type (%s) doesn't match generic type K (%s)", tmpIdField.getClass(), K.getClass()));
		idField = tmpIdField;
	}

	private List<Field> getFields(Class clazz, boolean recursuvely) {
		List<Field> allFields = new ArrayList<Field>();
		if (clazz.getDeclaredFields() != null && clazz.getDeclaredFields().length > 0) {
			for (Field field : clazz.getDeclaredFields()) {
				allFields.add(field);
			}
			if (recursuvely && clazz.getSuperclass() != null) {
				allFields.addAll(getFields(clazz.getSuperclass(), true));
			}
		}
		return allFields;
	}

    void fillMutationBatch(MutationBatch mb, ColumnFamily<K, String> columnFamily, T entity) {
		try {
			@SuppressWarnings("unchecked")
			K rowKey = (K) idField.get(entity);
			ColumnListMutation<String> clm = mb.withRow(columnFamily, rowKey);
			clm.setDefaultTtl(ttl);
			
			for (ColumnMapper mapper : columnList.values()) {
				mapper.fillMutationBatch(entity, clm, "");
			}
		} catch(Exception e) {
			throw new PersistenceException("failed to fill mutation batch", e);
		}
	}

	T constructEntity(K id, ColumnList<String> cl) {
		try {
		    T entity = clazz.newInstance();
			idField.set(entity, id);
			
			for (com.netflix.astyanax.model.Column<String> column : cl) {
			    List<String> name = Lists.newArrayList(StringUtils.split(column.getName(), "."));
			    setField(entity, name.iterator(), column);
			}
			
			for (ColumnMapper column : columnList.values()) {
			    column.validate(entity);
			}
			return entity;
		} catch(Exception e) {
			throw new PersistenceException("failed to construct entity", e);
		}
	}
	
	void setField(T entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
	    String fieldName = name.next();
	    ColumnMapper mapper = this.columnList.get(fieldName);
        if (mapper != null)
            mapper.setField(entity, name, column);
	}
	
	@SuppressWarnings("unchecked")
	K getEntityId(T entity) throws Exception {
	    return (K)idField.get(entity);
	}
	
	@VisibleForTesting
	Field getId() {
		return idField;
	}
	
	@VisibleForTesting
	Collection<ColumnMapper> getColumnList() {
		return columnList.values();
	}

	String getEntityName() {
	    return entityName;
	}
	
	@Override
	public String toString() {
		return String.format("EntityMapper(%s)", clazz);
	}
}
