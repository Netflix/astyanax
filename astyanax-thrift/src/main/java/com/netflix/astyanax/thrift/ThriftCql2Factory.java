package com.netflix.astyanax.thrift;

import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.query.CqlQuery;

public class ThriftCql2Factory implements ThriftCqlFactory {
    @Override
    public CqlStatement createCqlStatement(ThriftKeyspaceImpl keyspace) {
        return new ThriftCqlStatement(keyspace);
    }

    @Override
    public <K, C> CqlQuery<K, C> createCqlQuery(ThriftColumnFamilyQueryImpl<K, C> cfQuery, String cql) {
        return new ThriftCqlQuery<K, C>(cfQuery, cql);
    }
}
