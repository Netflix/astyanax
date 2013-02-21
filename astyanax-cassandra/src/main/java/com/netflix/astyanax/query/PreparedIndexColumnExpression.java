package com.netflix.astyanax.query;

public interface PreparedIndexColumnExpression<K, C> {
    /**
     * Set the column part of the expression
     * 
     * @param columnName
     * @return
     */
    PreparedIndexOperationExpression<K, C> whereColumn(C columnName);
}
