package com.netflix.astyanax.query;

public interface PreparedIndexOperationExpression<K, C> {
    PreparedIndexValueExpression<K, C> equals();

    PreparedIndexValueExpression<K, C> greaterThan();

    PreparedIndexValueExpression<K, C> lessThan();

    PreparedIndexValueExpression<K, C> greaterThanEquals();

    PreparedIndexValueExpression<K, C> lessThanEquals();

}
