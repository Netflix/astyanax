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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.SchemaDisagreementException;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.ddl.impl.SchemaChangeResponseImpl;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.thrift.ddl.*;

public class ThriftClusterImpl implements Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClusterImpl.class);
    
    private static final int MAX_SCHEMA_CHANGE_ATTEMPTS = 6;
    private static final int SCHEMA_DISAGREEMENT_BACKOFF = 10000;

    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final ConcurrentMap<String, Keyspace> keyspaces;
    private final AstyanaxConfiguration config;
    private final KeyspaceTracerFactory tracerFactory;

    public ThriftClusterImpl(
            AstyanaxConfiguration config, 
            ConnectionPool<Cassandra.Client> connectionPool,
            KeyspaceTracerFactory tracerFactory) {
        this.config         = config;
        this.connectionPool = connectionPool;
        this.tracerFactory  = tracerFactory;
        this.keyspaces      = Maps.newConcurrentMap();
    }

    @Override
    public String describeClusterName() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<String>(tracerFactory.newTracer(CassandraOperationType.DESCRIBE_CLUSTER)) {
                    @Override
                    public String internalExecute(Client client, ConnectionContext context) throws Exception {
                        return client.describe_cluster_name();
                    }

                }, config.getRetryPolicy().duplicate()).getResult();
    }

    @Override
    public String describeSnitch() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<String>(tracerFactory.newTracer(CassandraOperationType.DESCRIBE_SNITCH)) {
                    @Override
                    public String internalExecute(Client client, ConnectionContext context) throws Exception {
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
                            public String internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public Map<String, List<String>> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                    public String internalExecute(Client client, ConnectionContext state) throws Exception {
                        return client.describe_version();
                    }
                }, config.getRetryPolicy().duplicate()).getResult();
    }

    private <K> OperationResult<K> executeSchemaChangeOperation(AbstractOperationImpl<K> op) throws OperationException,
            ConnectionException {
        int attempt = 0;
        do {
            try {
                return connectionPool.executeWithFailover(op, config.getRetryPolicy().duplicate());
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
    public List<KeyspaceDefinition> describeKeyspaces() throws ConnectionException {
        return connectionPool.executeWithFailover(
                new AbstractOperationImpl<List<KeyspaceDefinition>>(
                        tracerFactory.newTracer(CassandraOperationType.DESCRIBE_KEYSPACES)) {
                    @Override
                    public List<KeyspaceDefinition> internalExecute(Client client, ConnectionContext context) throws Exception {
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
                keyspace = keyspaces.putIfAbsent(ksName, newKeyspace);
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
    public OperationResult<SchemaChangeResult> addColumnFamily(final ColumnFamilyDefinition def) throws ConnectionException {
        return internalCreateColumnFamily(((ThriftColumnFamilyDefinitionImpl) def)
                .getThriftColumnFamilyDefinition());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> createColumnFamily(final Map<String, Object> options) throws ConnectionException {
        final ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();
        def.setFields(options);
        
        return internalCreateColumnFamily(def.getThriftColumnFamilyDefinition());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> createColumnFamily(final Properties props) throws ConnectionException {
        final CfDef def;
        try {
            def = ThriftUtils.getThriftObjectFromProperties(CfDef.class, props);
        } catch (Exception e) {
            throw new BadRequestException("Error converting properties to CfDef", e);
        }
        
        return internalCreateColumnFamily(def);
    }
    
    private OperationResult<SchemaChangeResult> internalCreateColumnFamily(final CfDef def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                tracerFactory.newTracer(CassandraOperationType.ADD_COLUMN_FAMILY), def.getKeyspace()) {
            @Override
            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                precheckSchemaAgreement(client);
                return new SchemaChangeResponseImpl()
                    .setSchemaId(client.system_add_column_family(def));
            }
        });
    }

    @Override
    public OperationResult<SchemaChangeResult>  updateColumnFamily(final ColumnFamilyDefinition def) throws ConnectionException {
        return internalColumnFamily(((ThriftColumnFamilyDefinitionImpl) def).getThriftColumnFamilyDefinition());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> updateColumnFamily(final Map<String, Object> options) throws ConnectionException  {
        final ThriftColumnFamilyDefinitionImpl def = new ThriftColumnFamilyDefinitionImpl();
        def.setFields(options);
        
        return internalColumnFamily(def.getThriftColumnFamilyDefinition());
    }

    @Override
    public OperationResult<SchemaChangeResult> updateColumnFamily(final Properties props) throws ConnectionException {
        final CfDef def;
        try {
            def = ThriftUtils.getThriftObjectFromProperties(CfDef.class, props);
        } catch (Exception e) {
            throw new BadRequestException("Error converting properties to CfDef", e);
        }
        
        return internalColumnFamily(def);
    }
    
    private OperationResult<SchemaChangeResult>  internalColumnFamily(final CfDef def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                tracerFactory.newTracer(CassandraOperationType.UPDATE_COLUMN_FAMILY), def.getKeyspace()) {
            @Override
            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                precheckSchemaAgreement(client);
                return new SchemaChangeResponseImpl().setSchemaId(client.system_update_column_family(def));
            }
        });
    }

    @Override
    public KeyspaceDefinition makeKeyspaceDefinition() {
        return new ThriftKeyspaceDefinitionImpl();
    }

    @Override
    public OperationResult<SchemaChangeResult>  addKeyspace(final KeyspaceDefinition def) throws ConnectionException {
        return internalCreateKeyspace(((ThriftKeyspaceDefinitionImpl) def).getThriftKeyspaceDefinition());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(final Map<String, Object> options) throws ConnectionException  {
        final ThriftKeyspaceDefinitionImpl def = new ThriftKeyspaceDefinitionImpl();
        def.setFields(options);
        
        return internalCreateKeyspace(def.getThriftKeyspaceDefinition());
    }

    @Override
    public OperationResult<SchemaChangeResult> createKeyspace(final Properties props) throws ConnectionException {
        final KsDef def;
        try {
            def = ThriftUtils.getThriftObjectFromProperties(KsDef.class, props);
            if (def.getCf_defs() == null) {
                def.setCf_defs(Lists.<CfDef>newArrayList());
            }
        } catch (Exception e) {
            throw new BadRequestException("Error converting properties to KsDef", e);
        }
        
        return internalCreateKeyspace(def);
    }
    
    private OperationResult<SchemaChangeResult> internalCreateKeyspace(final KsDef def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractOperationImpl<SchemaChangeResult>(
                tracerFactory.newTracer(CassandraOperationType.ADD_KEYSPACE)) {
            @Override
            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                precheckSchemaAgreement(client);
                return new SchemaChangeResponseImpl()
                    .setSchemaId(client.system_add_keyspace(def));
            }
        });
    }

    @Override
    public OperationResult<SchemaChangeResult>  updateKeyspace(final KeyspaceDefinition def) throws ConnectionException {
        return internalUpdateKeyspace(((ThriftKeyspaceDefinitionImpl) def).getThriftKeyspaceDefinition());
    }
    
    @Override
    public OperationResult<SchemaChangeResult> updateKeyspace(final Map<String, Object> options) throws ConnectionException  {
        final ThriftKeyspaceDefinitionImpl def = new ThriftKeyspaceDefinitionImpl();
        try {
            def.setFields(options);
        } catch (Exception e) {
            throw new BadRequestException("Error converting properties to KsDef", e);
        }
        
        return internalUpdateKeyspace(def.getThriftKeyspaceDefinition());
    }

    @Override
    public OperationResult<SchemaChangeResult> updateKeyspace(final Properties props) throws ConnectionException {
        final KsDef def;
        try {
            def = ThriftUtils.getThriftObjectFromProperties(KsDef.class, props);
            if (def.getCf_defs() == null) {
                def.setCf_defs(Lists.<CfDef>newArrayList());
            }
        } catch (Exception e) {
            throw new BadRequestException("Error converting properties to KsDef", e);
        }
        return internalUpdateKeyspace(def);
    }

    private OperationResult<SchemaChangeResult> internalUpdateKeyspace(final KsDef def) throws ConnectionException {
        return executeSchemaChangeOperation(new AbstractOperationImpl<SchemaChangeResult>(
                tracerFactory.newTracer(CassandraOperationType.UPDATE_KEYSPACE)) {
            @Override
            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                precheckSchemaAgreement(client);
                return new SchemaChangeResponseImpl().setSchemaId(client.system_update_keyspace(def));
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
    
    @Override
    public OperationResult<SchemaChangeResult> dropColumnFamily(final String keyspaceName, final String columnFamilyName) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_COLUMN_FAMILY), keyspaceName) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_drop_column_family(columnFamilyName));
                            }
                        }, RunOnce.get());
    }

    @Override
    public OperationResult<SchemaChangeResult> dropKeyspace(final String keyspaceName) throws ConnectionException  {
        return connectionPool
                .executeWithFailover(
                        new AbstractKeyspaceOperationImpl<SchemaChangeResult>(
                                tracerFactory.newTracer(CassandraOperationType.DROP_KEYSPACE), keyspaceName) {
                            @Override
                            public SchemaChangeResult internalExecute(Client client, ConnectionContext context) throws Exception {
                                precheckSchemaAgreement(client);
                                return new SchemaChangeResponseImpl().setSchemaId(client.system_drop_keyspace(keyspaceName));
                            }
                        }, RunOnce.get());
    }

    @Override
    public Properties getAllKeyspaceProperties() throws ConnectionException {
        List<KeyspaceDefinition> keyspaces = this.describeKeyspaces();
        Properties props = new Properties();
        for (KeyspaceDefinition ksDef : keyspaces) {
            ThriftKeyspaceDefinitionImpl thriftKsDef = (ThriftKeyspaceDefinitionImpl)ksDef;
            try {
                for (Entry<Object, Object> prop : thriftKsDef.getProperties().entrySet()) {
                    props.setProperty(ksDef.getName() + "." + prop.getKey(), (String) prop.getValue());
                }
            } catch (Exception e) {
            }
        }
        return props;
    }

    @Override
    public Properties getKeyspaceProperties(String keyspace) throws ConnectionException {
        KeyspaceDefinition ksDef = this.describeKeyspace(keyspace);
        if (ksDef == null)
            throw new NotFoundException(String.format("Keyspace '%s' not found", keyspace));
        
        Properties props = new Properties();
        ThriftKeyspaceDefinitionImpl thriftKsDef = (ThriftKeyspaceDefinitionImpl)ksDef;
        try {
            for (Entry<Object, Object> prop : thriftKsDef.getProperties().entrySet()) {
                props.setProperty((String)prop.getKey(), (String) prop.getValue());
            }
        } catch (Exception e) {
            LOG.error(String.format("Error fetching properties for keyspace '%s'", keyspace));
        }
        return props;
    }

    @Override
    public Properties getColumnFamilyProperties(String keyspace, String columnFamily) throws ConnectionException {
        KeyspaceDefinition ksDef = this.describeKeyspace(keyspace);
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(columnFamily);
        if (cfDef == null)
            throw new NotFoundException(String.format("Column family '%s' in keyspace '%s' not found", columnFamily, keyspace));
        
        Properties props = new Properties();
        ThriftColumnFamilyDefinitionImpl thriftCfDef = (ThriftColumnFamilyDefinitionImpl)cfDef;
        try {
            for (Entry<Object, Object> prop : thriftCfDef.getProperties().entrySet()) {
                props.setProperty((String)prop.getKey(), (String) prop.getValue());
            }
        } catch (Exception e) {
            LOG.error("Error processing column family properties");
        }
        return props;
    }
    
    /**
     * Do a quick check to see if there is a schema disagreement.  This is done as an extra precaution
     * to reduce the chances of putting the cluster into a bad state.  This will not gurantee however, that 
     * by the time a schema change is made the cluster will be in the same state.
     * @param client
     * @throws Exception
     */
    private void precheckSchemaAgreement(Client client) throws Exception {
        Map<String, List<String>> schemas = client.describe_schema_versions();
        if (schemas.size() > 1) {
            throw new SchemaDisagreementException("Can't change schema due to pending schema agreement");
        }
    }
}
