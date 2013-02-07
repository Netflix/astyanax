package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

public class MapColumnMapper extends AbstractColumnMapper {
    private final Class<?>           keyClazz;
    private final Class<?>           valueClazz;
    private final Serializer<?>      keySerializer;
    private final Serializer<Object> valueSerializer;

    public MapColumnMapper(Field field) {
        super(field);
        
        ParameterizedType stringListType = (ParameterizedType) field.getGenericType();
        this.keyClazz         = (Class<?>) stringListType.getActualTypeArguments()[0];
        this.keySerializer    = SerializerTypeInferer.getSerializer(this.keyClazz);

        this.valueClazz       = (Class<?>) stringListType.getActualTypeArguments()[1];
        this.valueSerializer  = SerializerTypeInferer.getSerializer(this.valueClazz);
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception {
        Map<?, ?> map = (Map<?, ?>) field.get(entity);
        if (map == null) {
            if (columnAnnotation.nullable())
                return false; // skip
            else
                throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
        }
        
        for (Entry<?, ?> entry : map.entrySet()) {
            clm.putColumn(prefix + columnName + "." + entry.getKey().toString(), entry.getValue(), valueSerializer, null);
        }
        return true;
    }
    
    @Override
    public boolean setField(Object entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
        Map<Object, Object> map = (Map<Object, Object>) field.get(entity);
        if (map == null) {
            map = Maps.newLinkedHashMap();
            field.set(entity,  map);
        }
        
        String key = name.next();
        if (name.hasNext())
            return false;
        map.put(keySerializer.fromByteBuffer(keySerializer.fromString(key)),
                valueSerializer.fromByteBuffer(column.getByteBufferValue()));
        return true;
    }

    @Override
    public void validate(Object entity) throws Exception {
        // TODO Auto-generated method stub
    }

}
