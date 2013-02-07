package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

/**
 * 
 * <field>.<key>
 * @author elandau
 *
 */
public class SetColumnMapper extends AbstractColumnMapper {
    private final Class<?>      clazz;
    private final Serializer<?> serializer;

    public SetColumnMapper(Field field) {
        super(field);
        
        ParameterizedType stringListType = (ParameterizedType) field.getGenericType();
        this.clazz = (Class<?>) stringListType.getActualTypeArguments()[0];
        this.serializer       = SerializerTypeInferer.getSerializer(this.clazz);
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception {
        Set<?> set = (Set<?>) field.get(entity);
        if(set == null) {
            if(columnAnnotation.nullable())
                return false; // skip
            else
                throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
        }
        
        for (Object entry : set) {
            clm.putEmptyColumn(prefix + columnName + "." + entry.toString(), null);
        }
        return true;
    }
    
    @Override
    public boolean setField(Object entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
        Set<Object> set = (Set<Object>) field.get(entity);
        if (set == null) {
            set = Sets.newHashSet();
            field.set(entity,  set);
        }
        
        String value = name.next();
        if (name.hasNext())
            return false;
        set.add(serializer.fromByteBuffer(serializer.fromString(value)));
        return true;
    }

    @Override
    public void validate(Object entity) throws Exception {
        // TODO Auto-generated method stub
        
    }
}
