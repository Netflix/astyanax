package com.netflix.astyanax;

import org.apache.cassandra.dht.IPartitioner;

import com.google.common.base.Supplier;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;

/**
 * Factory that groups a family of Keyspace, Client and ConnectionFactory for a
 * specific RPC to cassandra (i.e. Thrift)
 * 
 * @author elandau
 * 
 * @param <T>
 */
public interface AstyanaxTypeFactory<T> {
    Keyspace createKeyspace(String ksName, ConnectionPool<T> cp, AstyanaxConfiguration asConfig,
            Supplier<IPartitioner> partitioner, KeyspaceTracerFactory tracerFactory);

    Cluster createCluster(ConnectionPool<T> cp, AstyanaxConfiguration asConfig, 
            KeyspaceTracerFactory tracerFactory);

    ConnectionFactory<T> createConnectionFactory(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cfConfig, 
            KeyspaceTracerFactory tracerFactory, ConnectionPoolMonitor monitor);
}
