/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
