package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Set;

import javax.persistence.Entity;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.serializers.SerializerTypeInferer;

public class MappingUtils {
	
    static com.netflix.astyanax.Serializer<?> getSerializerForField(Field field) {
        com.netflix.astyanax.Serializer<?> serializer = null;
        
        // check if there is explicit @Serializer annotation first
		if (hasSerializerAnnotation(field)) {
			serializer = retrieveSerializerFromAnnotation(field);
		} else {
			
			Class<?> type;
			if (isMapOrSet(field.getType())) {
				type = getGenericTypeClass(field);
			} else {
				type = field.getType();
			}
			serializer = SerializerTypeInferer.getSerializer(type);
		
		}
        return serializer;
    }

    private static com.netflix.astyanax.Serializer<?> retrieveSerializerFromAnnotation(Field field) {
    	Serializer serializerAnnotation = field.getAnnotation(Serializer.class);
        final Class<?> serializerClazz = serializerAnnotation.value();
        
        // check type
        if(!(com.netflix.astyanax.Serializer.class.isAssignableFrom(serializerClazz)))
            throw new RuntimeException("annotated serializer class is not a subclass of com.netflix.astyanax.Serializer. " + serializerClazz.getCanonicalName());
        
        // invoke public static get() method
        try {
            Method getInstanceMethod = serializerClazz.getMethod("get");
            return (com.netflix.astyanax.Serializer<?>) getInstanceMethod.invoke(null);
        } catch(Exception e) {
            throw new RuntimeException("Failed to get or invoke public static get() method", e);
        }
	}

	private static boolean hasSerializerAnnotation(Field field) {
    	return field.getAnnotation(Serializer.class)!=null;
	}

	private static boolean isMapOrSet(Class<?> type) {
    	boolean result = Map.class.isAssignableFrom(type);
		result |= Set.class.isAssignableFrom(type);
		
		return result;
	}
    
    static Class<?> getGenericTypeClass(Field field){
        ParameterizedType stringListType = (ParameterizedType) field.getGenericType();
        return (Class<?>) stringListType.getActualTypeArguments()[0];
    }
    
	static String getEntityName(Entity entityAnnotation, Class<?> clazz) {
        String name = entityAnnotation.name();
        if (name == null || name.isEmpty()) 
            return StringUtils.substringAfterLast(clazz.getName(), ".").toLowerCase();
        else
            return name;
    }
}
