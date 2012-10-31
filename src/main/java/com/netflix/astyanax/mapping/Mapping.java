package com.netflix.astyanax.mapping;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 * Utility for doing object/relational mapping between bean-like instances and
 * Cassandra
 * </p>
 * <p/>
 * <p>
 * The mapper stores values in Cassandra and maps in/out to native types. Column
 * names must be strings. Annotate your bean with {@link Id} and {@link Column}.
 * Or, provide an {@link AnnotationSet} that defines IDs and Columns in your
 * bean.
 */
@SuppressWarnings({ "SuspiciousMethodCalls" })
public class Mapping<T> {
    private final ImmutableMap<String, FieldMapping> fields;
    private final String idFieldName;
    private final Class<T> clazz;

    /**
     * If the ID column does not have a Column annotation, this column name is
     * used
     */
    public static final String DEFAULT_ID_COLUMN_NAME = "ID";

    /**
     * Convenience for allocation a mapping object
     * 
     * @param clazz
     *            clazz type to map
     * @return mapper
     */
    public static <T> Mapping<T> make(Class<T> clazz) {
        return new Mapping<T>(clazz, new DefaultAnnotationSet());
    }

    /**
     * Convenience for allocation a mapping object
     * 
     * @param clazz
     *            clazz type to map
     * @param annotationSet
     *            annotations to use when analyzing a bean
     * @return mapper
     */
    public static <T> Mapping<T> make(Class<T> clazz,
            AnnotationSet<?, ?> annotationSet) {
        return new Mapping<T>(clazz, annotationSet);
    }

    /**
     * @param clazz
     *            clazz type to map
     */
    public Mapping(Class<T> clazz) {
        this(clazz, new DefaultAnnotationSet());
    }

    /**
     * @param clazz
     *            clazz type to map
     * @param annotationSet
     *            annotations to use when analyzing a bean
     */
    public Mapping(Class<T> clazz, AnnotationSet<?, ?> annotationSet) {
        this.clazz = clazz;

        String localKeyFieldName = null;
        ImmutableMap.Builder<String, FieldMapping> builder = ImmutableMap
                .builder();

        AtomicBoolean isKey = new AtomicBoolean();
        Set<String> usedNames = Sets.newHashSet();
        for (Field field : clazz.getDeclaredFields()) {
            String name = mapField(field, annotationSet, builder, usedNames,
                    isKey);
            if (isKey.get()) {
                Preconditions.checkArgument(localKeyFieldName == null);
                localKeyFieldName = name;
            }
        }

        Preconditions.checkNotNull(localKeyFieldName);

        fields = builder.build();
        idFieldName = localKeyFieldName;
    }

    /**
     * Return the value for the ID/Key column from the given instance
     * 
     * @param instance
     *            the instance
     * @param valueClass
     *            type of the value (must match the actual native type in the
     *            instance's class)
     * @return value
     */
    public <V> V getIdValue(T instance, Class<V> valueClass) {
        return getColumnValue(instance, idFieldName, valueClass);
    }

    /**
     * Return the value for the given column from the given instance
     * 
     * @param instance
     *            the instance
     * @param columnName
     *            name of the column (must match a corresponding annotated field
     *            in the instance's class)
     * @param valueClass
     *            type of the value (must match the actual native type in the
     *            instance's class)
     * @return value
     */
    public <V> V getColumnValue(T instance, String columnName,
            Class<V> valueClass) {
        FieldMapping fieldMapping = fields.get(columnName);
        if (fieldMapping == null) {
            throw new IllegalArgumentException("Column not found: "
                    + columnName);
        }
        try {
            return valueClass.cast(fieldMapping.getField().get(instance));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e); // should never get here
        }
    }

    /**
     * Set the value for the ID/Key column for the given instance
     * 
     * @param instance
     *            the instance
     * @param value
     *            The value (must match the actual native type in the instance's
     *            class)
     */
    public <V> void setIdValue(T instance, V value) {
        setColumnValue(instance, idFieldName, value);
    }

    /**
     * Set the value for the given column for the given instance
     * 
     * @param instance
     *            the instance
     * @param columnName
     *            name of the column (must match a corresponding annotated field
     *            in the instance's class)
     * @param value
     *            The value (must match the actual native type in the instance's
     *            class)
     */
    public <V> void setColumnValue(T instance, String columnName, V value) {
        FieldMapping fieldMapping = fields.get(columnName);
        if (fieldMapping == null) {
            throw new IllegalArgumentException("Column not found: "
                    + columnName);
        }
        try {
            fieldMapping.getField().set(instance, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e); // should never get here
        }
    }

    /**
     * Map a bean to a column mutation. i.e. set the columns in the mutation to
     * the corresponding values from the instance
     * 
     * @param instance
     *            instance
     * @param mutation
     *            mutation
     */
    public void fillMutation(T instance, ColumnListMutation<String> mutation) {
        for (String fieldName : getNames()) {
            FieldMapping fieldMapping = fields.get(fieldName);
            Coercions.setColumnMutationFromField(instance,
                    fieldMapping.getField(), fieldName, mutation,
                    fieldMapping.getTtl());
        }
    }

    /**
     * Allocate a new instance and populate it with the values from the given
     * column list
     * 
     * @param columns
     *            column list
     * @return the allocated instance
     * @throws IllegalAccessException
     *             if a new instance could not be instantiated
     * @throws InstantiationException
     *             if a new instance could not be instantiated
     */
    public T newInstance(ColumnList<String> columns)
            throws IllegalAccessException, InstantiationException {
        return initInstance(clazz.newInstance(), columns);
    }

    /**
     * Populate the given instance with the values from the given column list
     * 
     * @param instance
     *            instance
     * @param columns
     *            column this
     * @return instance (as a convenience for chaining)
     */
    public T initInstance(T instance, ColumnList<String> columns) {
        for (com.netflix.astyanax.model.Column<String> column : columns) {
            FieldMapping fieldMapping = fields.get(column.getName());
            if (fieldMapping != null) { // otherwise it may be a column that was
                                        // removed, etc.
                Coercions.setFieldFromColumn(instance, fieldMapping.getField(),
                        column);
            }
        }
        return instance;
    }

    /**
     * Load a set of rows into new instances populated with values from the
     * column lists
     * 
     * @param rows
     *            the rows
     * @return list of new instances
     * @throws IllegalAccessException
     *             if a new instance could not be instantiated
     * @throws InstantiationException
     *             if a new instance could not be instantiated
     */
    public List<T> getAll(Rows<?, String> rows) throws InstantiationException,
            IllegalAccessException {
        List<T> list = Lists.newArrayList();
        for (Row<?, String> row : rows) {
            if (!row.getColumns().isEmpty()) {
                list.add(newInstance(row.getColumns()));
            }
        }
        return list;
    }

    /**
     * Return the set of column names discovered from the bean class
     * 
     * @return column names
     */
    public Collection<String> getNames() {
        return fields.keySet();
    }

    Class<?> getIdFieldClass() {
        return fields.get(idFieldName).getField().getType();
    }

    private <ID extends Annotation, COLUMN extends Annotation> String mapField(
            Field field, AnnotationSet<ID, COLUMN> annotationSet,
            ImmutableMap.Builder<String, FieldMapping> builder,
            Set<String> usedNames, AtomicBoolean isKey) {
        String mappingName = null;

        ID idAnnotation = field.getAnnotation(annotationSet.getIdAnnotation());
        COLUMN columnAnnotation = field.getAnnotation(annotationSet
                .getColumnAnnotation());

        if ((idAnnotation != null) && (columnAnnotation != null)) {
            throw new IllegalStateException(
                    "A field cannot be marked as both an ID and a Column: "
                            + field.getName());
        }

        if (idAnnotation != null) {
            mappingName = annotationSet.getIdName(field, idAnnotation);
            isKey.set(true);
        } else {
            isKey.set(false);
        }

        int ttl = -1;
        if ((columnAnnotation != null)) {
            mappingName = annotationSet.getColumnName(field, columnAnnotation);
            if (annotationSet instanceof AnnotationSet2) {
                ttl = ((AnnotationSet2<ID, COLUMN>) annotationSet)
                        .getColumnTtl(field, columnAnnotation);
            }
        }

        if (mappingName != null) {
            Preconditions.checkArgument(
                    !usedNames.contains(mappingName.toLowerCase()), mappingName
                            + " has already been used for this column family");
            usedNames.add(mappingName.toLowerCase());

            field.setAccessible(true);
            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setField(field);
            if (ttl > -1) {
                fieldMapping.setTtl(ttl);
            }
            builder.put(mappingName, fieldMapping);
        }

        return mappingName;
    }
}
