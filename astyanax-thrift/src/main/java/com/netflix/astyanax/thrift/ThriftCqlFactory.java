package com.netflix.astyanax.thrift;

import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.query.CqlQuery;

public interface ThriftCqlFactory {
    public CqlStatement createCqlStatement(ThriftKeyspaceImpl keyspace);
    
    public <K, C> CqlQuery<K, C> createCqlQuery(ThriftColumnFamilyQueryImpl<K, C> cfQuery, String cql);
}
