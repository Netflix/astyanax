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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.KsDef;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.SchemaDisagreementException;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.thrift.ddl.*;

public class ThriftClusterImpl implements Cluster {

    private static final int MAX_SCHEMA_CHANGE_ATTEMPTS = 6;
    private static final int SCHEMA_DISAGREEMENT_BACKOFF = 10000;

    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final ConcurrentMap<String, Keyspace> keyspaces;
    private final AstyanaxConfiguration config;
    private final KeyspaceTracerFactory tracerFactory;

    public ThriftClusterImpl(AstyanaxConfiguration config, ConnectionPool<Cassandra.Client> connectionPool,
            KeyspaceTracerFactory tracerFactory) {
        this.config = config;
        this.connectionPool = connectionPool;
        this.tracerFactory = tracerFactory;
        this.keyspaces = Maps.newConcurrentMap();
    }

    @Override
    public String describeClusterName() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<String>(tracerFactory.newTracer(CassandraOperationType.DESCRIBE_CLUSTER)) {
                    @Override
                    public String internalExecute(Client client) throws Exception {
                        return client.describe_cluster_name();
                    }

                }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public String describeSnitch() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<String>(tracerFactory.newTracer(CassandraOperationType.DESCRIBE_SNITCH)) {
                    @Override
                    public String internalExecute(Client client) throws Exception {
                        return client.describe_snitch();
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public String describePartitioner() throws ConnectionException {
        return connectionPool
                .executeWithFailover(
                        new AbstractOperationImpl<String>(
                                tracerFactory.newTracer(CassandraOperationType.DESCRIBE_PARTITIONER)) {
                            @Override
                            public String internalExecute(Client client) throws Exception {
                                return client.describe_partitioner();
                            }
                        }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<Map<String, List<String>>>(
                        tracerFactory.newTracer(CassandraOperationType.DESCRIBE_SCHEMA_VERSION)) {
                    @Override
                    public Map<String, List<String>> internalExecute(Client client) throws Exception {
                        return client.describe_schema_versions();
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    /**
     * Get the version from the cluster
     *
     * @return
     * @throws OperationException
     */
    @Override
    public String getVersion() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<String>(tracerFactory.newTracer(CassandraOperationType.GET_VERSION)) {
                    @Override
                    public String internalExecute(Client client) throws Exception {
                        return client.describe_version();
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    private <K> K executeSchemaChangeOperation(AbstractOperationImpl<K> op) throws OperationException,
            ConnectionException {
        int attempt = 0;
        do {
            try {
                return connectionPool.executeWithFailover(op, config.getRetryPolicy().duplicate()).getResult();
            }
            catch (SchemaDisagreementException e) {
                if (++attempt >= MAX_SCHEMA_CHANGE_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(SCHEMA_DISAGREEMENT_BACKOFF);
                }
                catch (InterruptedException e1) {
                    Thread.interrupted();
                    throw new RuntimeException(e1);
                }
            }
        } while (true);
    }

    @Override
    public String dropColumnFamily(final String keyspaceName, final String columnFamilyName) throws OperationException,
            ConnectionException {
        return executeSchemaChangeOperation(new AbstractKeyspaceOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.DROP_COLUMN_FAMILY), keyspaceName) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client.system_drop_column_family(columnFamilyName);
            }
        });
    }

    @Override
    public String dropKeyspace(final String keyspaceName) throws OperationException, ConnectionException {
        return executeSchemaChangeOperation(new AbstractOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.DROP_KEYSPACE)) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client.system_drop_keyspace(keyspaceName);
            }
        });
    }

    @Override
    public List<KeyspaceDefinition> describeKeyspaces() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<List<KeyspaceDefinition>>(
                        tracerFactory.newTracer(CassandraOperationType.DESCRIBE_KEYSPACES)) {
                    @Override
                    public List<KeyspaceDefinition> internalExecute(Client client) throws Exception {
                        List<KsDef> ksDefs = client.describe_keyspaces();
                        return Lists.transform(ksDefs, new Function<KsDef, KeyspaceDefinition>() {
                            @Override
                            public KeyspaceDefinition apply(KsDef ksDef) {
                                return new ThriftKeyspaceDefinitionImpl(ksDef);
                            }

                        });
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public KeyspaceDefinition describeKeyspace(String ksName) throws ConnectionException {
        List<KeyspaceDefinition> ksDefs = describeKeyspaces();
        for (KeyspaceDefinition ksDef : ksDefs) {
            if (ksDef.getName().equals(ksName)) {
                return ksDef;
            }
        }
        return null;
    }

    @Override
    public Keyspace getKeyspace(String ksName) {
        Keyspace keyspace = keyspaces.get(ksName);
        if (keyspace == null) {
            synchronized (this) {
                Keyspace newKeyspace = new ThriftKeyspaceImpl(ksName, this.connectionPool, this.config, tracerFactory);
                keyspace = keyspaces.put(ksName, newKeyspace);
                if (keyspace == null) {
                    keyspace = newKeyspace;
                }
            }
        }
        return keyspace;
    }

    @Override
    public ColumnFamilyDefinition makeColumnFamilyDefinition() {
        return new ThriftColumnFamilyDefinitionImpl();
    }

    @Override
    public String addColumnFamily(final ColumnFamilyDefinition def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractKeyspaceOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), def.getKeyspace()) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client.system_add_column_family(((ThriftColumnFamilyDefinitionImpl) def)
                        .getThriftColumnFamilyDefinition());
            }
        });
    }

    @Override
    public String updateColumnFamily(final ColumnFamilyDefinition def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractKeyspaceOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.UPDATE_COLUMN_FAMILY), def.getKeyspace()) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client.system_update_column_family(((ThriftColumnFamilyDefinitionImpl) def)
                        .getThriftColumnFamilyDefinition());
            }
        });
    }

    @Override
    public KeyspaceDefinition makeKeyspaceDefinition() {
        return new ThriftKeyspaceDefinitionImpl();
    }

    @Override
    public String addKeyspace(final KeyspaceDefinition def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.ADD_KEYSPACE)) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client.system_add_keyspace(((ThriftKeyspaceDefinitionImpl) def).getThriftKeyspaceDefinition());
            }
        });
    }

    @Override
    public String updateKeyspace(final KeyspaceDefinition def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractOperationImpl<String>(
                tracerFactory.newTracer(CassandraOperationType.UPDATE_KEYSPACE)) {
            @Override
            public String internalExecute(Client client) throws Exception {
                return client
                        .system_update_keyspace(((ThriftKeyspaceDefinitionImpl) def).getThriftKeyspaceDefinition());
            }
        });
    }

    @Override
    public ColumnDefinition makeColumnDefinition() {
        return new ThriftColumnDefinitionImpl();
    }

    @Override
    public AstyanaxConfiguration getConfig() {
        return config;
    }
}
