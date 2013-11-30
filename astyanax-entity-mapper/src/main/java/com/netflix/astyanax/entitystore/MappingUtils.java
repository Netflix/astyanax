package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

public class MappingUtils {
	
	/**
	 * will be inferred from the type of Field if field has no explicit {@link Serializer}
	 * @param field
	 * @return
	 */
    static com.netflix.astyanax.Serializer<?> getSerializerForField(Field field) {
    	
       return getSerializer(field, field.getType());
    }
    
    /**
     * will be inferred from the argument clazz if field has no explicit {@link Serializer}
     * @param field
     * @param clazz
     * @return
     */
    static com.netflix.astyanax.Serializer<?> getSerializer(Field field, Class<?> clazz) {
        com.netflix.astyanax.Serializer<?> serializer = null;
        
        // check if there is explicit @Serializer annotation first
		if (hasSerializerAnnotation(field)) {
			serializer = retrieveSerializerFromAnnotation(field);
		} else {
			serializer = SerializerTypeInferer.getSerializer(clazz);
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
    
	static Class<?> getGenericTypeClass(Field field, int index){
		ParameterizedType stringListType = (ParameterizedType) field.getGenericType();
		Type[] actualTypes = stringListType.getActualTypeArguments();
		Preconditions.checkElementIndex(index, actualTypes.length);
		
        return (Class<?>) stringListType.getActualTypeArguments()[index];
	}
    
	static String getEntityName(Entity entityAnnotation, Class<?> clazz) {
        String name = entityAnnotation.name();
        if (name == null || name.isEmpty()) 
            return StringUtils.substringAfterLast(clazz.getName(), ".").toLowerCase();
        else
            return name;
    }
	
	static int countColumnFields(Class<?> clazz){
		Field[] fields = clazz.getDeclaredFields();
		
		int count=0;
		for(Field field : fields){
			if(field.isAnnotationPresent(Column.class)){
				count +=1;
			}
		}
		
		return count;
	}
}
