package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;

class CompositeColumnMapper extends AbstractColumnMapper {
	private final Class<?> clazz;
	private final Map<String, ColumnMapper> columnList;

	CompositeColumnMapper(final Field field) {
	    super(field);
		this.clazz = field.getType();
		// clazz should be annotated with @Entity
		Entity entityAnnotation = clazz.getAnnotation(Entity.class);
		if(entityAnnotation == null)
			throw new IllegalArgumentException("class is NOT annotated with @java.persistence.Entity: " + clazz.getName());

		columnList = Maps.newHashMapWithExpectedSize(clazz.getDeclaredFields().length);
		Set<String> usedColumnNames = Sets.newHashSet();
		for (Field childField : clazz.getDeclaredFields()) {
			// extract @Column annotated fields
			Column annotation = childField.getAnnotation(Column.class);
			if ((annotation != null)) {
				childField.setAccessible(true);
				ColumnMapper columnMapper = null;
				Entity compositeAnnotation = childField.getType().getAnnotation(Entity.class);
				if(compositeAnnotation == null) {
					columnMapper = new LeafColumnMapper(childField);
				} else {
					columnMapper = new CompositeColumnMapper(childField);
				}
				Preconditions.checkArgument(!usedColumnNames.contains(columnMapper.getColumnName().toLowerCase()), 
						String.format("duplicate case-insensitive column name: %s", columnMapper.getColumnName()));
				columnList.put(columnMapper.getColumnName(), columnMapper);
				usedColumnNames.add(columnMapper.getColumnName().toLowerCase());
			}
		}
	}

	@Override
	public String toString() {
		return String.format("CompositeColumnMapper(%s)", clazz);
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception {
		Object childEntity = field.get(entity);
		if(childEntity == null) {
			if(columnAnnotation.nullable()) {
				return false; // skip. cannot write null column
			} else {
				throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
			}
		}
		
		prefix += getColumnName() + ".";
		boolean hasNonNullChildField = false;
		for (ColumnMapper mapper : columnList.values()) {
			boolean childFilled = mapper.fillMutationBatch(childEntity, clm, prefix);
			if(childFilled)
				hasNonNullChildField = true;
		}
		return hasNonNullChildField;
	}

//	@Override
//	public boolean setField(Object entity, ColumnList<String> cl) throws Exception {
//		Object childEntity = clazz.newInstance();
//		boolean hasNonNullChildField = false;
//		for (ColumnMapper mapper : columnList.values()) {
//			boolean childSet = mapper.setField(childEntity, cl);
//			if(childSet)
//				hasNonNullChildField = true;
//		}	
//		if(hasNonNullChildField) {
//			field.set(entity, childEntity);
//			return true;
//		} else {
//			if(columnAnnotation.nullable()) {
//				return false; // skip. keep it null
//			} else {
//				// if not-nullable nested entity has all null child fields 
//				// then we didn't write any columns to cassandra.
//				// but since it's non-nullable,
//				// we should still set the constructed default object
//				field.set(entity, childEntity);
//				return true;
//			}
//		}
//	}

    @Override
    public boolean setField(Object entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
        Object childEntity = field.get(entity);
        if (childEntity == null) {
            childEntity = clazz.newInstance();
            field.set(entity, childEntity);
        }
        
        ColumnMapper mapper = this.columnList.get(name.next());
        if (mapper == null)
            return false;
        
        return mapper.setField(childEntity, name, column);
    }

}
