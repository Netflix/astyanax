package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PersistenceException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.ColumnPredicate;

/**
 * The composite entity mapper expects an entity which has a single one to many
 * relationship with another entity representing the composite columns.  The child
 * entity will have an @Id field for each part of the composite followed by a single
 * @Column for the value.
 * 
 * @author elandau
 *
 * @param <T>
 * @param <K>
 */
public class CompositeEntityMapper<T, K> {

    private final Class<T>              clazz;
    private final Integer               ttl;
    private final Method                ttlMethod;
    private final FieldMapper<?>        idMapper;
    private final String                entityName;
    private final CompositeColumnEntityMapper embeddedEntityMapper;

    /**
     * 
     * @param clazz
     * @throws IllegalArgumentException 
     *      if clazz is NOT annotated with @Entity
     *      if column name contains illegal char (like dot)
     */
    public CompositeEntityMapper(Class<T> clazz, Integer ttl) {
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
                tempIdMapper = new FieldMapper(field);
            }
            
            // Should be a 'list'
            OneToMany oneToManyAnnotation = field.getAnnotation(OneToMany.class);
            if (oneToManyAnnotation != null) {
                Preconditions.checkArgument(tempEmbeddedEntityMapper == null, "there are multiple fields with @OneToMany annotation");
                tempEmbeddedEntityMapper = new CompositeColumnEntityMapper(field);
            }
        }
        
        Preconditions.checkNotNull(tempIdMapper, "there are no field with @Id annotation");
        idMapper    = tempIdMapper;
        
        Preconditions.checkNotNull(tempEmbeddedEntityMapper, "there are no embedded entities using @OneToMany annotation");
        embeddedEntityMapper    = tempEmbeddedEntityMapper;
    }

    public void fillMutationBatch(MutationBatch mb, ColumnFamily<K, ByteBuffer> columnFamily, T entity) {
        try {
            @SuppressWarnings("unchecked")
            ColumnListMutation<ByteBuffer> clm = mb.withRow(columnFamily, (K)idMapper.getValue(entity));
            clm.setDefaultTtl(getTtl(entity));
            embeddedEntityMapper.fillMutationBatch(clm, entity);
        } catch(Exception e) {
            throw new PersistenceException("failed to fill mutation batch", e);
        }
    }
    
    void fillMutationBatchForDelete(MutationBatch mb, ColumnFamily<K, ByteBuffer> columnFamily, T entity) {
        try {
            ByteBuffer id         = idMapper.toByteBuffer(entity);
            @SuppressWarnings("unchecked")
            ColumnListMutation<ByteBuffer> clm = mb.withRow(columnFamily, (K)idMapper.getValue(entity));
            embeddedEntityMapper.fillMutationBatchForDelete(clm, entity);
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
     * Construct an entity object from a row key and column list.
     * 
     * @param id
     * @param cl
     * @return
     */
    public T constructEntity(K id, ColumnList<ByteBuffer> cl) {
        try {
            // First, construct the parent class and give it an id
            T entity = clazz.newInstance();
            idMapper.setValue(entity, id);
            
            // Now map to the OneToMany embedded entity field
            embeddedEntityMapper.setField(entity, cl);
            
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    public T constructEntityFromCql(T candidateEntity, ColumnList<ByteBuffer> cl) {
        try {
            K id = (K) idMapper.fromByteBuffer(Iterables.getFirst(cl, null).getByteBufferValue());
            T entity;
            if (candidateEntity != null && idMapper.getValue(candidateEntity).equals(id)) {
                entity = candidateEntity;
            }
            else {
                entity = clazz.newInstance();
            }
            
            // First, construct the parent class and give it an id
            idMapper.setValue(entity, id);
            
            // Now map to the OneToMany embedded entity field
            embeddedEntityMapper.setFieldFromCql(entity, cl);
            
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    public String getComparatorType() {
        return embeddedEntityMapper.getComparatorType();
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
    
    public String getValueType() {
        return embeddedEntityMapper.getValueType();
    }

    public ByteBuffer[] getQueryEndpoints(Collection<ColumnPredicate> predicates) {
        return this.embeddedEntityMapper.getQueryEndpoints(predicates);
    }
}
