package com.netflix.astyanax.entitystore;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.query.ColumnPredicate;

/**
 * SQL'ish like fluent API for defining a query.  This is mainly used by the various entity mappers
 * to query for a subset of columns.  Each entity mapper stores data differently and will use the
 * predicates here to make the correct lower level query.
 * 
 * @author elandau
 *
 * @param <T>
 * @param <K>
 */
public abstract class NativeQuery<T, K> {
    protected List<K>                 ids = Lists.newArrayList();
    protected Collection<Object>      columnNames;
    protected List<ColumnPredicate>   predicates;
    protected int                     columnLimit = Integer.MAX_VALUE;
    
    /**
     * Refine query for row key
     * @author elandau
     *
     */
    public class NativeIdQuery {
        public NativeQuery<T, K> in(Collection<K> keys) {
            ids.addAll(keys);
            return NativeQuery.this;
        }
        
        public NativeQuery<T, K> in(K... keys) {
            ids.addAll(Lists.newArrayList(keys));
            return NativeQuery.this;
        }
        
        public NativeQuery<T, K> equal(K key) {
            ids.add(key);
            return NativeQuery.this;
        }
    }
    
    /**
     * Refine query for column range or slice
     * @author elandau
     *
     */
    public class NativeColumnQuery {
        private ColumnPredicate predicate = new ColumnPredicate();
        
        public NativeColumnQuery(String name) {
            predicate.setName(name);
        }
        
        public NativeQuery<T, K> in(Collection<Object> names) {
            columnNames = names;
            return NativeQuery.this;
        }

        public NativeQuery<T, K> equal(Object value) {
            return addPredicate(predicate.setOp(Equality.EQUAL).setValue(value));
        }
        
        public NativeQuery<T, K> greaterThan(Object value) {
            return addPredicate(predicate.setOp(Equality.GREATER_THAN).setValue(value));
        }
        
        public NativeQuery<T, K> lessThan(Object value) {
            return addPredicate(predicate.setOp(Equality.LESS_THAN).setValue(value));
        }
        
        public NativeQuery<T, K> greaterThanEqual(Object value) {
            return addPredicate(predicate.setOp(Equality.GREATER_THAN_EQUALS).setValue(value));
        }
        
        public NativeQuery<T, K> lessThanEqual(Object value) {
            return addPredicate(predicate.setOp(Equality.LESS_THAN_EQUALS).setValue(value));
        }
        
    }
    
    public NativeIdQuery whereId() {
        return new NativeIdQuery();
    }
    
    public NativeColumnQuery whereColumn(String name) {
        return new NativeColumnQuery(name);
    }

    public NativeQuery<T,K> limit(int columnLimit) {
        this.columnLimit = columnLimit;
        return this;
    }
    
    private NativeQuery<T, K> addPredicate(ColumnPredicate predicate) {
        if (predicates == null) {
            predicates = Lists.newArrayList();
        }
        
        predicates.add(predicate);
        return this;
    }

    /**
     * Return a single entity (or first) response
     * @return
     * @throws Exception
     */
    public abstract T getSingleResult() throws Exception;
    
    /**
     * Return a result set of entities
     * @return
     * @throws Exception
     */
    public abstract Collection<T> getResultSet() throws Exception;
    
    /**
     * Get the result set as a mapping of the id field to a collection of entities. This
     * is useful for a multi-get scenario where it is desirable to group all the 'entities'
     * within a row.
     * 
     * @return
     * @throws Exception
     */
    public abstract Map<K, Collection<T>> getResultSetById() throws Exception;

    /**
     * Get the column count for each id in the query without sending data back
     * to the client.
     * @return
     * @throws Excerption
     */
    public abstract Map<K, Integer> getResultSetCounts() throws Exception;
}
