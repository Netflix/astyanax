/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
    public ConnectionFactory<Cassandra.Client> createConnectionFactory(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cfConfig,
            KeyspaceTracerFactory tracerFactory, ConnectionPoolMonitor monitor) {
        return (ConnectionFactory<Cassandra.Client>) new ThriftSyncConnectionFactoryImpl(asConfig, cfConfig, tracerFactory,
                monitor);
    }

}
