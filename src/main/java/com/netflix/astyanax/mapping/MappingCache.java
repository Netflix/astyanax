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
    public <T> Mapping<T> getMapping(Class<T> clazz) {
        return getMapping(clazz, new DefaultAnnotationSet());
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
            AnnotationSet<?, ?> annotationSet) {
        Mapping<T> mapping = (Mapping<T>) cache.get(clazz); // cast is safe as
                                                            // this instance is
                                                            // the one adding to
                                                            // the map
        if (mapping == null) {
            // multiple threads can get here but that's OK
            mapping = new Mapping<T>(clazz, annotationSet);
            cache.put(clazz, mapping);
        }

        return mapping;
    }
}
