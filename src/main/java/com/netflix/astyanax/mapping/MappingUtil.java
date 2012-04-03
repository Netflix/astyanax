package com.netflix.astyanax.mapping;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

import java.util.List;

/**
 * Higher level mapping functions. Methods that behave similar to a Map.
 */
public class MappingUtil {
    private final Keyspace keyspace;
    private final MappingCache cache;
    private final AnnotationSet<?, ?> annotationSet;

    /**
     * @param keyspace
     *            keyspace to use
     */
    public MappingUtil(Keyspace keyspace) {
        this(keyspace, null, null);
    }

    /**
     * @param keyspace
     *            keyspace to use
     * @param annotationSet
     *            annotation set to use
     */
    public MappingUtil(Keyspace keyspace, AnnotationSet<?, ?> annotationSet) {
        this(keyspace, null, annotationSet);
    }

    /**
     * @param keyspace
     *            keyspace to use
     * @param cache
     *            cache to use
     */
    public MappingUtil(Keyspace keyspace, MappingCache cache) {
        this(keyspace, cache, null);
    }

    /**
     * @param keyspace
     *            keyspace to use
     * @param cache
     *            cache to use
     * @param annotationSet
     *            annotation set to use
     */
    public MappingUtil(Keyspace keyspace, MappingCache cache,
            AnnotationSet<?, ?> annotationSet) {
        this.keyspace = keyspace;
        this.cache = cache;
        this.annotationSet = (annotationSet != null) ? annotationSet
                : new DefaultAnnotationSet();
    }

    /**
     * Remove the given item
     * 
     * @param columnFamily
     *            column family of the item
     * @param item
     *            the item to remove
     * @throws Exception
     *             errors
     */
    public <T, K> void remove(ColumnFamily<K, String> columnFamily, T item)
            throws Exception {
        @SuppressWarnings({ "unchecked" })
        Class<T> clazz = (Class<T>) item.getClass();
        Mapping<T> mapping = getMapping(clazz);
        @SuppressWarnings({ "unchecked" })
        Class<K> idFieldClass = (Class<K>) mapping.getIdFieldClass(); // safe -
                                                                      // after
                                                                      // erasure,
                                                                      // this is
                                                                      // all
                                                                      // just
                                                                      // Class
                                                                      // anyway

        MutationBatch mutationBatch = keyspace.prepareMutationBatch();
        mutationBatch.withRow(columnFamily,
                mapping.getIdValue(item, idFieldClass)).delete();
        mutationBatch.execute();
    }

    /**
     * Add/update the given item
     * 
     * @param columnFamily
     *            column family of the item
     * @param item
     *            the item to add/update
     * @throws Exception
     *             errors
     */
    public <T, K> void put(ColumnFamily<K, String> columnFamily, T item)
            throws Exception {
        @SuppressWarnings({ "unchecked" })
        Class<T> clazz = (Class<T>) item.getClass();
        Mapping<T> mapping = getMapping(clazz);
        @SuppressWarnings({ "unchecked" })
        Class<K> idFieldClass = (Class<K>) mapping.getIdFieldClass(); // safe -
                                                                      // after
                                                                      // erasure,
                                                                      // this is
                                                                      // all
                                                                      // just
                                                                      // Class
                                                                      // anyway

        MutationBatch mutationBatch = keyspace.prepareMutationBatch();
        ColumnListMutation<String> columnListMutation = mutationBatch.withRow(
                columnFamily, mapping.getIdValue(item, idFieldClass));
        mapping.fillMutation(item, columnListMutation);

        mutationBatch.execute();
    }

    /**
     * Get the specified item by its key/id
     * 
     * @param columnFamily
     *            column family of the item
     * @param id
     *            id/key of the item
     * @param itemClass
     *            item's class
     * @return new instance with the item's columns propagated
     * @throws Exception
     *             errors
     */
    public <T, K> T get(ColumnFamily<K, String> columnFamily, K id,
            Class<T> itemClass) throws Exception {
        Mapping<T> mapping = getMapping(itemClass);
        ColumnList<String> result = keyspace.prepareQuery(columnFamily)
                .getKey(id).execute().getResult();
        return mapping.newInstance(result);
    }

    /**
     * Get all rows of the specified item
     * 
     * @param columnFamily
     *            column family of the item
     * @param itemClass
     *            item's class
     * @return new instances with the item's columns propagated
     * @throws Exception
     *             errors
     */
    public <T, K> List<T> getAll(ColumnFamily<K, String> columnFamily,
            Class<T> itemClass) throws Exception {
        Mapping<T> mapping = getMapping(itemClass);
        Rows<K, String> result = keyspace.prepareQuery(columnFamily)
                .getAllRows().execute().getResult();
        return mapping.getAll(result);
    }

    /**
     * Return the mapping instance for the given class
     * 
     * @param clazz
     *            the class
     * @return mapping instance (new or from cache)
     */
    public <T> Mapping<T> getMapping(Class<T> clazz) {
        return (cache != null) ? cache.getMapping(clazz, annotationSet)
                : new Mapping<T>(clazz, annotationSet);
    }
}
