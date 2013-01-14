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
			throw new IllegalArgumentException("class is NOT annotated with @Entity: " + clazz.getName());

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
	public void fillMutationBatch(Object entity, ColumnListMutation<String> clm) throws Exception {
		Object childEntity = field.get(entity);
		for (ColumnMapper mapper : columnList) {
			mapper.fillMutationBatch(childEntity, clm);
		}
	}

	@Override
	public void setField(Object entity, ColumnList<String> cl) throws Exception {
		Object childEntity = clazz.newInstance();
		for (ColumnMapper mapper : columnList) {
			mapper.setField(childEntity, cl);
		}
		field.set(entity, childEntity);
	}

}
