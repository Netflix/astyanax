package com.netflix.astyanax.thrift;

import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.query.CqlQuery;

public class ThriftCql3Factory implements ThriftCqlFactory {
    @Override
    public CqlStatement createCqlStatement(ThriftKeyspaceImpl keyspace) {
        return new ThriftCql3Statement(keyspace);
    }

    @Override
    public <K, C> CqlQuery<K, C> createCqlQuery(ThriftColumnFamilyQueryImpl<K, C> cfQuery, String cql) {
        return new ThriftCql3Query<K,C>(cfQuery, cql);
    }
}
