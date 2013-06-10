package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.query.ColumnPredicate;

/**
 * The composite entity mapper maps a Pojo to a composite column structure where
 * the row key represents the Pojo ID, each @Column is a component of the composite
 * and the final @Column is the column value.
 * @Column for the value.
 * 
 * @author elandau
 *
 * @param <T>
 * @param <K>
 */
public class CompositeEntityMapper<T, K> {

    /**
     * Entity class
     */
    private final Class<T>          clazz;
    
    /**
     * Default ttl
     */
    private final Integer           ttl;
    
    /**
     * TTL supplier method
     */
    private final Method            ttlMethod;
    
    /**
     * ID Field (same as row key)
     */
    final FieldMapper<?>            idMapper;
    
    /**
     * TODO
     */
    private final String            entityName;

    /**
     * List of serializers for the composite parts
     */
    private List<FieldMapper<?>>    components = Lists.newArrayList();
    
    /**
     * List of valid (i.e. existing) column names
     */
    private Set<String>             validNames = Sets.newHashSet();

    /**
     * Mapper for the value part of the entity
     */
    private FieldMapper<?>          valueMapper;
    
    /**
     * Largest buffer size
     */
    private int                     bufferSize = 64;

    /**
     * 
     * @param clazz
     * @param prefix 
     * @throws IllegalArgumentException 
     *      if clazz is NOT annotated with @Entity
     *      if column name contains illegal char (like dot)
     */
    public CompositeEntityMapper(Class<T> clazz, Integer ttl, ByteBuffer prefix) {
        this.clazz = clazz;
        
        // clazz should be annotated with @Entity
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if(entityAnnotation == null)
            throw new IllegalArgumentException("class is NOT annotated with @java.persistence.Entity: " + clazz.getName());
        
        entityName = MappingUtils.getEntityName(entityAnnotation, clazz);
        
        // TTL value from constructor or class-level annotation
        Integer tmpTtlValue = ttl;
        if(tmpTtlValue == null) {
            // constructor value has higher priority
            // try @TTL annotation at entity/class level.
            // it doesn't make sense to support @TTL annotation at individual column level.
            TTL ttlAnnotation = clazz.getAnnotation(TTL.class);
            if(ttlAnnotation != null) {
                int ttlAnnotationValue = ttlAnnotation.value();
                Preconditions.checkState(ttlAnnotationValue > 0, "cannot define non-positive value for TTL annotation at class level: " + ttlAnnotationValue);
                tmpTtlValue = ttlAnnotationValue;
            }
        }
        this.ttl = tmpTtlValue;

        // TTL method
        Method tmpTtlMethod = null;
        for (Method method : this.clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(TTL.class)) {
                Preconditions.checkState(tmpTtlMethod == null, "Duplicate TTL method annotation on " + method.getName());
                tmpTtlMethod = method;
                tmpTtlMethod.setAccessible(true);
            }
        }
        this.ttlMethod = tmpTtlMethod;

        Field[] declaredFields = clazz.getDeclaredFields();
        FieldMapper tempIdMapper = null;
        CompositeColumnEntityMapper tempEmbeddedEntityMapper = null;
        for (Field field : declaredFields) {
            // Should only have one id field and it should map to the row key
            Id idAnnotation = field.getAnnotation(Id.class);
            if(idAnnotation != null) {
                Preconditions.checkArgument(tempIdMapper == null, "there are multiple fields with @Id annotation");
                field.setAccessible(true);
                tempIdMapper = new FieldMapper(field, prefix);
            }
            
            // Composite part or the value
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation != null) {
                field.setAccessible(true);
                FieldMapper fieldMapper = new FieldMapper(field);
                components.add(fieldMapper);
                validNames.add(fieldMapper.getName());
            }
        }
        
        Preconditions.checkNotNull(tempIdMapper, "there are no field with @Id annotation");
        idMapper    = tempIdMapper;
        
        Preconditions.checkNotNull(components.size() > 2, "there should be at least 2 component columns and a value");
        
        // Last one is always treated as the 'value'
        valueMapper = components.remove(components.size() - 1);
    }

    void fillMutationBatch(MutationBatch mb, ColumnFamily<K, ByteBuffer> columnFamily, T entity) {
        try {
            @SuppressWarnings("unchecked")
            ColumnListMutation<ByteBuffer> clm = mb.withRow(columnFamily, (K)idMapper.getValue(entity));
            clm.setDefaultTtl(getTtl(entity));
            try {
                ByteBuffer columnName = toColumnName(entity);
                ByteBuffer value      = valueMapper.toByteBuffer(entity);
                clm.putColumn(columnName, value);
            } catch(Exception e) {
                throw new PersistenceException("failed to fill mutation batch", e);
            }

        } catch(Exception e) {
            throw new PersistenceException("failed to fill mutation batch", e);
        }
    }
    
    void fillMutationBatchForDelete(MutationBatch mb, ColumnFamily<K, ByteBuffer> columnFamily, T entity) {
        try {
            @SuppressWarnings("unchecked")
            ColumnListMutation<ByteBuffer> clm = mb.withRow(columnFamily, (K)idMapper.getValue(entity));
            clm.deleteColumn(toColumnName(entity));
        } catch(Exception e) {
            throw new PersistenceException("failed to fill mutation batch", e);
        }
    }
    
    private Integer getTtl(T entity) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Integer retTtl = this.ttl;
        // TTL method has higher priority
        if(ttlMethod != null) {
            Object retobj = ttlMethod.invoke(entity);
            retTtl = (Integer) retobj;
        }
        return retTtl;
    }
    
    /**
     * Return the column name byte buffer for this entity
     * 
     * @param obj
     * @return
     */
    private ByteBuffer toColumnName(Object obj) {
        SimpleCompositeBuilder composite = new SimpleCompositeBuilder(bufferSize, Equality.EQUAL);

        // Iterate through each component and add to a CompositeType structure
        for (FieldMapper<?> mapper : components) {
            try {
                composite.addWithoutControl(mapper.toByteBuffer(obj));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return composite.get();
    }
    
    /**
     * Construct an entity object from a row key and column list.
     * 
     * @param id
     * @param cl
     * @return
     */
    T constructEntity(K id, com.netflix.astyanax.model.Column<ByteBuffer> column) {
        try {
            // First, construct the parent class and give it an id
            T entity = clazz.newInstance();
            idMapper.setValue(entity, id);
            setEntityFieldsFromColumnName(entity, column.getRawName().duplicate());
            valueMapper.setField(entity, column.getByteBufferValue().duplicate());
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    T constructEntityFromCql(ColumnList<ByteBuffer> cl) {
        try {
            T entity = clazz.newInstance();
            
            // First, construct the parent class and give it an id
            K id = (K) idMapper.fromByteBuffer(Iterables.getFirst(cl, null).getByteBufferValue());
            idMapper.setValue(entity, id);
            
            Iterator<com.netflix.astyanax.model.Column<ByteBuffer>> columnIter = cl.iterator();
            columnIter.next();
            
            for (FieldMapper<?> component : components) {
                component.setField(entity, columnIter.next().getByteBufferValue());
            }
                    
            valueMapper.setField(entity, columnIter.next().getByteBufferValue());
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public K getEntityId(T entity) throws Exception {
        return (K)idMapper.getValue(entity);
    }
    
    @VisibleForTesting
    Field getId() {
        return idMapper.field;
    }
    
    public String getEntityName() {
        return entityName;
    }
    
    @Override
    public String toString() {
        return String.format("EntityMapper(%s)", clazz);
    }

    public String getKeyType() {
        return idMapper.getSerializer().getComparatorType().getClassName();
    }

    /**
     * Return an object from the column
     * 
     * @param cl
     * @return
     */
    Object fromColumn(K id, com.netflix.astyanax.model.Column<ByteBuffer> c) {
        try {
            // Allocate a new entity
            Object entity         = clazz.newInstance();
            
            idMapper.setValue(entity, id);
            setEntityFieldsFromColumnName(entity, c.getRawName().duplicate());
            valueMapper.setField(entity, c.getByteBufferValue().duplicate());
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    /**
     * 
     * @param entity
     * @param columnName
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    void setEntityFieldsFromColumnName(Object entity, ByteBuffer columnName) throws IllegalArgumentException, IllegalAccessException {
        // Iterate through components in order and set fields
        for (FieldMapper<?> component : components) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null) {
                if (data.remaining() > 0) {
                    component.setField(entity, data);
                }
                byte end_of_component = columnName.get();
                if (end_of_component != Equality.EQUAL.toByte()) {
                    throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                }
            }
            else {
                throw new RuntimeException("Missing component data in composite type");
            }
        }
    }

    /**
     * Return the cassandra comparator type for this composite structure
     * @return
     */
    public String getComparatorType() {
        StringBuilder sb = new StringBuilder();
        sb.append("CompositeType(");
        sb.append(StringUtils.join(
            Collections2.transform(components, new Function<FieldMapper<?>, String>() {
                public String apply(FieldMapper<?> input) {
                    return input.serializer.getComparatorType().getClassName();
                }
            }),
            ","));
        sb.append(")");
        return sb.toString();
    }

    
    public static int getShortLength(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    public static ByteBuffer getWithShortLength(ByteBuffer bb) {
        int length = getShortLength(bb);
        return getBytes(bb, length);
    }

    public static ByteBuffer getBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    String getValueType() {
        return valueMapper.getSerializer().getComparatorType().getClassName();
    }

    ByteBuffer[] getQueryEndpoints(Collection<ColumnPredicate> predicates) {
        // Convert to multimap for easy lookup
        ArrayListMultimap<Object, ColumnPredicate> lookup = ArrayListMultimap.create();
        for (ColumnPredicate predicate : predicates) {
            Preconditions.checkArgument(validNames.contains(predicate.getName()), "Field '" + predicate.getName() + "' does not exist in the entity " + clazz.getCanonicalName());
            lookup.put(predicate.getName(), predicate);
        }
        
        SimpleCompositeBuilder start = new SimpleCompositeBuilder(bufferSize, Equality.GREATER_THAN_EQUALS);
        SimpleCompositeBuilder end   = new SimpleCompositeBuilder(bufferSize, Equality.LESS_THAN_EQUALS);

        // Iterate through components in order while applying predicate to 'start' and 'end'
        for (FieldMapper<?> mapper : components) {
            for (ColumnPredicate p : lookup.get(mapper.getName())) {
                try {
                    applyPredicate(mapper, start, end, p);
                }
                catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to serialize predicate '%s'", p.toString()), e);
                }
            }
        }
        
        return new ByteBuffer[]{start.get(), end.get()};
    }
    
    void applyPredicate(FieldMapper<?> mapper, SimpleCompositeBuilder start, SimpleCompositeBuilder end, ColumnPredicate predicate) {
        ByteBuffer bb = mapper.valueToByteBuffer(predicate.getValue());
        
        switch (predicate.getOp()) {
        case EQUAL:
            start.addWithoutControl(bb);
            end.addWithoutControl(bb);
            break;
        case GREATER_THAN:
        case GREATER_THAN_EQUALS:
            if (mapper.isAscending())
                start.add(bb, predicate.getOp());
            else 
                end.add(bb, predicate.getOp());
            break;
        case LESS_THAN:
        case LESS_THAN_EQUALS:
            if (mapper.isAscending())
                end.add(bb, predicate.getOp());
            else 
                start.add(bb, predicate.getOp());
            break;
        }
    }
}
