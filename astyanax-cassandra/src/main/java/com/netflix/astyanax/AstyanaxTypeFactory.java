/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax;

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
            KeyspaceTracerFactory tracerFactory);

    Cluster createCluster(ConnectionPool<T> cp, AstyanaxConfiguration asConfig, 
            KeyspaceTracerFactory tracerFactory);

    ConnectionFactory<T> createConnectionFactory(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cfConfig, 
            KeyspaceTracerFactory tracerFactory, ConnectionPoolMonitor monitor);
}
