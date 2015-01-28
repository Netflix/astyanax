package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
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

    @SuppressWarnings("unchecked")
	public MapColumnMapper(Field field) {
        super(field);
        
        /**
         * Only support custom type for value for now
         * Hoping there will something like @Serializer(mapKey=KeySerializer, mapValue=ValueSerializer)
         * or @MapSerializer(key=KeySerializer, value=ValueSerialier) to specify serializer for both key and value
         * in the future, then declared fields could be Map<Foo, Foo>.
         */
        this.keyClazz         = MappingUtils.getGenericTypeClass(field, 0);
        this.keySerializer    = SerializerTypeInferer.getSerializer(keyClazz);
        
        this.valueClazz       = MappingUtils.getGenericTypeClass(field, 1);
        this.valueSerializer  = (Serializer<Object>) MappingUtils.getSerializer(field, valueClazz);
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
