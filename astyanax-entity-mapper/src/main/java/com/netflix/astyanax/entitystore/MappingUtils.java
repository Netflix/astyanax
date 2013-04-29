package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.persistence.Entity;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.serializers.SerializerTypeInferer;

public class MappingUtils {
    static com.netflix.astyanax.Serializer<?> getSerializerForField(Field field) {
        com.netflix.astyanax.Serializer<?> serializer = null;
        // check if there is explicit @Serializer annotation first
        Serializer serializerAnnotation = field.getAnnotation(Serializer.class);
        if(serializerAnnotation != null) {
            final Class<?> serializerClazz = serializerAnnotation.value();
            // check type
            if(!(com.netflix.astyanax.Serializer.class.isAssignableFrom(serializerClazz)))
                throw new RuntimeException("annotated serializer class is not a subclass of com.netflix.astyanax.Serializer. " + serializerClazz.getCanonicalName());
            // invoke public static get() method
            try {
                Method getInstanceMethod = serializerClazz.getMethod("get");
                serializer = (com.netflix.astyanax.Serializer<?>) getInstanceMethod.invoke(null);
            } catch(Exception e) {
                throw new RuntimeException("Failed to get or invoke public static get() method", e);
            }
        } else {
            // otherwise automatically infer the Serializer type from field object type
            serializer = SerializerTypeInferer.getSerializer(field.getType());
        }
        return serializer;
    }

    static String getEntityName(Entity entityAnnotation, Class<?> clazz) {
        String name = entityAnnotation.name();
        if (name == null || name.isEmpty()) 
            return StringUtils.substringAfterLast(clazz.getName(), ".").toLowerCase();
        else
            return name;
    }


}
