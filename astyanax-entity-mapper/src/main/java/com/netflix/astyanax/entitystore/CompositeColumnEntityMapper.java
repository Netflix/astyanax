package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

/**
 * Mapper from a CompositeType to an embedded entity.  The composite entity is expected
 * to have an @Id annotation for each composite component and a @Column annotation for
 * the value.
 * 
 * @author elandau
 *
 */
public class CompositeColumnEntityMapper {
    private final static int  COMPONENT_OVERHEAD = 3;
    private final static byte END_OF_COMPONENT   = 0;
    
    /**
     * Class of embedded entity
     */
    private final Class<?>          clazz;
    
    /**
     * List of serializers for the composite parts
     */
    private List<FieldMapper<?>>    components = Lists.newArrayList();
    
    /**
     * Mapper for the value part of the entity
     */
    private FieldMapper<?>          valueMapper;
    
    /**
     * Largest buffer size
     */
    private int                     bufferSize = 64;
    
    /**
     * Parent field
     */
    private final Field                   containerField;
    
    public CompositeColumnEntityMapper(Field field) {
        
        ParameterizedType containerEntityType = (ParameterizedType) field.getGenericType();
        
        this.clazz  = (Class<?>) containerEntityType.getActualTypeArguments()[0];
        this.containerField = field;
        this.containerField.setAccessible(true);
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field f : declaredFields) {
            // One of these for each component of the composite
            Id idAnnotation = f.getAnnotation(Id.class);
            if (idAnnotation != null) {
                f.setAccessible(true);
                components.add(new FieldMapper(f));
            }
            
            // The value
            Column columnAnnotation = f.getAnnotation(Column.class);
            if ((columnAnnotation != null)) {
                Preconditions.checkArgument(valueMapper == null, "there should only be one value field");
                f.setAccessible(true);
                valueMapper = new FieldMapper(f);
            }
        }        
    }
    
    /**
     * Iterate through the list and create a column for each element
     * @param clm
     * @param entity
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public void fillMutationBatch(ColumnListMutation<ByteBuffer> clm, Object entity) throws IllegalArgumentException, IllegalAccessException {
        List<?> list = (List<?>) containerField.get(entity);
        if (list != null) {
            for (Object element : list) {
                fillColumnMutation(clm, element);
            }
        }
    }
    
    public void fillMutationBatchForDelete(ColumnListMutation<ByteBuffer> clm, Object entity) throws IllegalArgumentException, IllegalAccessException {
        List<?> list = (List<?>) containerField.get(entity);
        if (list == null) {
            clm.delete();
        }
        else {
            for (Object element : list) {
                clm.deleteColumn(toColumnName(element));
            }
        }
    }
    
    /**
     * Add a column based on the provided entity
     * 
     * @param clm
     * @param entity
     */
    public void fillColumnMutation(ColumnListMutation<ByteBuffer> clm, Object entity) {
        try {
            ByteBuffer columnName = toColumnName(entity);
            ByteBuffer value      = valueMapper.toByteBuffer(entity);
            
            clm.putColumn(columnName, value);
        } catch(Exception e) {
            throw new PersistenceException("failed to fill mutation batch", e);
        }
    }
    
    /**
     * Return the column name byte buffer for this entity
     * 
     * @param obj
     * @return
     */
    public ByteBuffer toColumnName(Object obj) {
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);

        // Iterate through each component and add to a CompositeType structure
        for (FieldMapper mapper : components) {
            try {
                // First, serialize the ByteBuffer for this component
                ByteBuffer cb = mapper.toByteBuffer(obj);
                if (cb == null) {
                    cb = ByteBuffer.allocate(0);
                }

                if (cb.limit() + COMPONENT_OVERHEAD > bb.remaining()) {
                    int exponent = (int) Math.ceil(Math.log((double) (cb.limit() + COMPONENT_OVERHEAD + bb.limit())) / Math.log(2));
                    int newBufferSize = (int) Math.pow(2, exponent);
                    ByteBuffer temp = ByteBuffer.allocate(newBufferSize);
                    bb.flip();
                    temp.put(bb);
                    bb = temp;
                }
                // Write the data: <length><data><0>
                bb.putShort((short) cb.remaining());
                bb.put(cb.slice());
                bb.put(END_OF_COMPONENT);
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        bb.flip();
        return bb;
    }
    
    /**
     * Set the collection field using the provided column list of embedded entities
     * @param entity
     * @param name
     * @param column
     * @return
     * @throws Exception
     */
    public boolean setField(Object entity, ColumnList<ByteBuffer> columns) throws Exception {
        List<Object> list = getOrCreateField(entity);
            
        // Iterate through columns and add embedded entities to the list
        for (com.netflix.astyanax.model.Column<ByteBuffer> c : columns) {
            list.add(fromColumn(c));
        }
        
        return true;
    }
    
    public boolean setFieldFromCql(Object entity, ColumnList<ByteBuffer> columns) throws Exception {
        List<Object> list = getOrCreateField(entity);
    
        // Iterate through columns and add embedded entities to the list
//        for (com.netflix.astyanax.model.Column<ByteBuffer> c : columns) {
            list.add(fromCqlColumns(columns));
//       }
        
        return true;
    }
    
    private List<Object> getOrCreateField(Object entity) throws IllegalArgumentException, IllegalAccessException {
        // Get or create the list field
        List<Object> list = (List<Object>) containerField.get(entity);
        if (list == null) {
            list = Lists.newArrayList();
            containerField.set(entity,  list);
        }
        return list;
    }
    
    /**
     * Return an object from the column
     * 
     * @param cl
     * @return
     */
    public Object fromColumn(com.netflix.astyanax.model.Column<ByteBuffer> c) {
        try {
            // Allocate a new entity
            Object entity         = clazz.newInstance();
            
            setEntityFieldsFromColumnName(entity, c.getRawName().duplicate());
            
            valueMapper.setField(entity, c.getByteBufferValue().duplicate());
            return entity;
        } catch(Exception e) {
            throw new PersistenceException("failed to construct entity", e);
        }
    }
    
    public Object fromCqlColumns(com.netflix.astyanax.model.ColumnList<ByteBuffer> c) {
        try {
            // Allocate a new entity
            Object entity         = clazz.newInstance();
            
            Iterator<com.netflix.astyanax.model.Column<ByteBuffer>> columnIter = c.iterator();
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
    
    /**
     * 
     * @param entity
     * @param columnName
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public void setEntityFieldsFromColumnName(Object entity, ByteBuffer columnName) throws IllegalArgumentException, IllegalAccessException {
        // Iterate through components in order and set fields
        for (FieldMapper component : components) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null) {
                if (data.remaining() > 0) {
                    component.setField(entity, data);
                }
                byte end_of_component = columnName.get();
                if (end_of_component != END_OF_COMPONENT) {
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

    public String getValueType() {
        return valueMapper.getSerializer().getComparatorType().getClassName();
    }
}
