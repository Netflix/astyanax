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
package com.netflix.astyanax.mapping;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Utility to cache mappers. There's a small performance hit to reflect on a
 * bean. This cache, re-uses mappers for a given bean
 */
public class MappingCache {
    private final Map<Class<?>, Mapping<?>> cache = Maps.newConcurrentMap();

    /**
     * Return a new or cached mapper
     * 
     * @param clazz
     *            class for the mapper
     * @return mapper
     */
    public <T> Mapping<T> getMapping(Class<T> clazz, boolean includeParentFields) {
        return getMapping(clazz, new DefaultAnnotationSet(), includeParentFields);
    }

    public <T> Mapping<T> getMapping(Class<T> clazz) {
		return getMapping(clazz, false);		
	}

    /**
     * Return a new or cached mapper
     * 
     * @param clazz
     *            class for the mapper
     * @param annotationSet
     *            annotation set for the mapper
     * @return mapper
     */
    @SuppressWarnings({ "unchecked" })
    public <T> Mapping<T> getMapping(Class<T> clazz,
            AnnotationSet<?, ?> annotationSet, boolean includeParentFields) {
        Mapping<T> mapping = (Mapping<T>) cache.get(clazz); // cast is safe as
                                                            // this instance is
                                                            // the one adding to
                                                            // the map
        if (mapping == null) {
            // multiple threads can get here but that's OK
            mapping = new Mapping<T>(clazz, annotationSet, includeParentFields);
            cache.put(clazz, mapping);
        }

        return mapping;
    }

    public <T> Mapping<T> getMapping(Class<T> clazz, AnnotationSet<?, ?> annotationSet) {
		return getMapping(clazz, annotationSet, false);
	}
}
