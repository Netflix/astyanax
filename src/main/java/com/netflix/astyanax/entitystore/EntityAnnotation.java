package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * utility class to map btw Entity annotations and cassandra data model
 * @param <T> entity type 
 * @param <K> rowKey type
 */
class EntityAnnotation {

	private final Class<?> clazz;
	private final Field idField;
	private final ImmutableMap<String, Field> columns;

	EntityAnnotation(Class<?> clazz) {
		this.clazz = clazz;

		Field tmpIdField = null;
		ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
		Set<String> lowerCaseColumnNames = Sets.newHashSet();
		for (Field field : clazz.getDeclaredFields()) {
			Id idAnnotation = field.getAnnotation(Id.class);
			Column columnAnnotation = field.getAnnotation(Column.class);

			if ((idAnnotation != null) && (columnAnnotation != null)) {
				throw new IllegalArgumentException("field cannot be marked as both an Id and a Column: " + field.getName());
			}

			if(idAnnotation != null) {
				Preconditions.checkArgument(tmpIdField == null, "there are multiple fields with @Id annotation");
				field.setAccessible(true);
				tmpIdField = field;
			}

			if ((columnAnnotation != null)) {
				String columnName = getColumnName(columnAnnotation, field);
				Preconditions.checkArgument(!lowerCaseColumnNames.contains(columnName.toLowerCase()), String.format("duplicate case-insensitive column name: %s", columnName));
				lowerCaseColumnNames.add(columnName.toLowerCase());
				field.setAccessible(true);
				builder.put(columnName, field);
			}
		}

		Preconditions.checkNotNull(tmpIdField, "there are no field with @Id annotation");
		//Preconditions.checkArgument(tmpIdField.getClass().equals(K.getClass()), String.format("@Id field type (%s) doesn't match generic type K (%s)", tmpIdField.getClass(), K.getClass()));
		idField = tmpIdField;

		this.columns = builder.build();
	}
	
	private String getColumnName(Column annotation, Field field) {
		// use field name if annotation name is not set
		String name = annotation.name();
        return (name.length() > 0) ? name : field.getName();
	}

	Field getId() {
		return idField;
	}

	Map<String, Field> getColumns() {
		return columns;
	}
	
	@Override
	public String toString() {
		return String.format("EntityMapper(%s)", clazz);
	}
}
