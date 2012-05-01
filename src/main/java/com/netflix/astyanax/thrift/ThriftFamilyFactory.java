package com.netflix.astyanax.thrift;

import org.apache.cassandra.thrift.Cassandra;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.AstyanaxTypeFactory;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;

public class ThriftFamilyFactory implements AstyanaxTypeFactory<Cassandra.Client> {

    private final static ThriftFamilyFactory instance = new ThriftFamilyFactory();

    public static ThriftFamilyFactory getInstance() {
        return instance;
    }

    @Override
    public Keyspace createKeyspace(String ksName, ConnectionPool<Cassandra.Client> cp, AstyanaxConfiguration asConfig,
            KeyspaceTracerFactory tracerFactory) {
        return new ThriftKeyspaceImpl(ksName, cp, asConfig, tracerFactory);
    }

    @Override
    public Cluster createCluster(ConnectionPool<Cassandra.Client> cp, AstyanaxConfiguration asConfig,
            KeyspaceTracerFactory tracerFactory) {
        return new ThriftClusterImpl(asConfig, (ConnectionPool<Cassandra.Client>) cp, tracerFactory);
    }

    @Override
    public ConnectionFactory<Cassandra.Client> createConnectionFactory(ConnectionPoolConfiguration cfConfig,
            KeyspaceTracerFactory tracerFactory, ConnectionPoolMonitor monitor) {
        return (ConnectionFactory<Cassandra.Client>) new ThriftSyncConnectionFactoryImpl(cfConfig, tracerFactory,
                monitor);
    }

}
