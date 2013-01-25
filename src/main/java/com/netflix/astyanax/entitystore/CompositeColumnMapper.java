package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

class CompositeColumnMapper extends AbstractColumnMapper {
	private final Field field;
	private final Column columnAnnotation;
	private final String columnName;
	private final Class<?> clazz;
	private final List<ColumnMapper> columnList;

	CompositeColumnMapper(final Field field, final String prefix) {
		this.field = field;
		this.columnAnnotation = field.getAnnotation(Column.class);
		this.columnName = deriveColumnName(columnAnnotation, field, prefix);
		
		this.clazz = field.getType();
		// clazz should be annotated with @Entity
		Entity entityAnnotation = clazz.getAnnotation(Entity.class);
		if(entityAnnotation == null)
			throw new IllegalArgumentException("class is NOT annotated with @java.persistence.Entity: " + clazz.getName());

		columnList = Lists.newArrayListWithCapacity(clazz.getDeclaredFields().length);
		Set<String> usedColumnNames = Sets.newHashSet();
		for (Field childField : clazz.getDeclaredFields()) {
			// extract @Column annotated fields
			Column annotation = childField.getAnnotation(Column.class);
			if ((annotation != null)) {
				childField.setAccessible(true);
				ColumnMapper columnMapper = null;
				Entity compositeAnnotation = childField.getType().getAnnotation(Entity.class);
				if(compositeAnnotation == null) {
					columnMapper = new LeafColumnMapper(childField, columnName);
				} else {
					columnMapper = new CompositeColumnMapper(childField, columnName);
				}
				Preconditions.checkArgument(!usedColumnNames.contains(columnMapper.getColumnName().toLowerCase()), 
						String.format("duplicate case-insensitive column name: %s", columnMapper.getColumnName()));
				columnList.add(columnMapper);
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
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm) throws Exception {
		Object childEntity = field.get(entity);
		if(childEntity == null) {
			if(columnAnnotation.nullable()) {
				return false; // skip. cannot write null column
			} else {
				throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
			}
		}
		boolean hasNonNullChildField = false;
		for (ColumnMapper mapper : columnList) {
			boolean childFilled = mapper.fillMutationBatch(childEntity, clm);
			if(childFilled)
				hasNonNullChildField = true;
		}
		return hasNonNullChildField;
	}

	@Override
	public boolean setField(Object entity, ColumnList<String> cl) throws Exception {
		Object childEntity = clazz.newInstance();
		boolean hasNonNullChildField = false;
		for (ColumnMapper mapper : columnList) {
			boolean childSet = mapper.setField(childEntity, cl);
			if(childSet)
				hasNonNullChildField = true;
		}	
		if(hasNonNullChildField) {
			field.set(entity, childEntity);
			return true;
		} else {
			if(columnAnnotation.nullable()) {
				return false; // skip. keep it null
			} else {
				// if not-nullable nested entity has all null child fields 
				// then we didn't write any columns to cassandra.
				// but since it's non-nullable,
				// we should still set the constructed default object
				field.set(entity, childEntity);
				return true;
			}
		}
	}

}
