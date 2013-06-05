package com.netflix.astyanax.recipes.queue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

/**
 * Very simple message queue manager using a set of column families to manage the queues
 * 
 * 1.  _info     - Contains the queue names and their metadata.  One row per queue.  
 *                 Row key is the queue name
 * 2.  _queue    - Contains the queue shards.  One row per shard. The row key contains the
 *                 queue name as well as the shard info.  
 * 3.  _metadata - Contains message metadata including the message body as well as references
 *                 to queue items in the shards.  One row per message.
 * 4.  _history  - History information for a message with a key.  One row per message.  One column
 *                 per history item.
 * 
 * @author elandau
 *
 */
public class SimpleMessageQueueManager implements MessageQueueManager {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessageQueueManager.class);
    
    public static final String           DEFAULT_MANAGER_NAME            = "Queues";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             SCHEMA_CHANGE_DELAY             = 3000;
    public static final ImmutableMap<String, Object> DEFAULT_COLUMN_FAMILY_SETTINGS = ImmutableMap.<String, Object>builder()
            .put("read_repair_chance",       1.0)
            .put("gc_grace_seconds",         5)     // TODO: Calculate gc_grace_seconds
            .put("compaction_strategy",      "SizeTieredCompactionStrategy")
            .build();

    /**
     *
     * @author elandau
     */
    public static class Builder {
        private String                          managerName          = DEFAULT_MANAGER_NAME;
        private ConsistencyLevel                consistencyLevel     = DEFAULT_CONSISTENCY_LEVEL;
        private Map<String, Object>             columnFamilySettings = DEFAULT_COLUMN_FAMILY_SETTINGS;
        private Keyspace                        keyspace;
                                              
        public Builder withManagerName(String managerName) {
            this.managerName = managerName;
            return this;
        }

        public Builder withConsistencyLevel(ConsistencyLevel level) {
            this.consistencyLevel = level;
            return this;
        }

        public Builder withColumnFamilySettings(Map<String, Object> settings) {
            this.columnFamilySettings = settings;
            return this;
        }

        public Builder withKeyspace(Keyspace keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public SimpleMessageQueueManager build() throws MessageQueueException {
            Preconditions.checkNotNull(keyspace, "Must specify keyspace");
                return new SimpleMessageQueueManager(this);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    private final  Keyspace                                     keyspace;
    private final  Map<String, Object>                          columnFamilySettings;
    private final  EntityManager<MessageQueueInfo, String>      queueEntityManager;
    private final  String                                       managerName;

    final ColumnFamily<String, MessageQueueEntry>     queueColumnFamily;
    final ColumnFamily<String, MessageMetadataEntry>  metadataColumnFamily;
    final ColumnFamily<String, UUID>                  historyColumnFamily;

    final static AnnotatedCompositeSerializer<MessageQueueEntry> entrySerializer
        = new AnnotatedCompositeSerializer<MessageQueueEntry>(MessageQueueEntry.class);
    final static AnnotatedCompositeSerializer<MessageMetadataEntry>   metadataSerializer
        = new AnnotatedCompositeSerializer<MessageMetadataEntry>(MessageMetadataEntry.class);
    
    public SimpleMessageQueueManager(Builder builder) {
        this.queueColumnFamily    = ColumnFamily.newColumnFamily(builder.managerName + MessageQueueConstants.CF_QUEUE_SUFFIX,    StringSerializer.get(), entrySerializer);
        this.metadataColumnFamily = ColumnFamily.newColumnFamily(builder.managerName + MessageQueueConstants.CF_INFO_SUFFIX,     StringSerializer.get(), metadataSerializer);
        this.historyColumnFamily  = ColumnFamily.newColumnFamily(builder.managerName + MessageQueueConstants.CF_HISTORY_SUFFIX,  StringSerializer.get(), TimeUUIDSerializer.get());

        this.managerName          = builder.managerName;
        this.columnFamilySettings = builder.columnFamilySettings;
        this.keyspace             = builder.keyspace;
        
        queueEntityManager = DefaultEntityManager.<MessageQueueInfo, String>builder()
                .withKeyspace(builder.keyspace)
                .withEntityType(MessageQueueInfo.class)
                .withConsistency(builder.consistencyLevel)
                .withColumnFamily(builder.managerName + MessageQueueConstants.CF_METADATA_SUFFIX)
                .build();
    }
    
    @Override
    public MessageQueueInfo readQueueInfo(String name) {
        return queueEntityManager.get(name);
    }

    @Override
    public void createMessageQueue(MessageQueueInfo info) {
        if (info.getColumnFamilyBase() == null)
            info.setColumnFamilyBase(this.managerName);
        
        queueEntityManager.put(info);
        
        if (!info.getColumnFamilyBase().equals(this.managerName)) {
           // TODO:  Create a separate set of column families for this queue
        }
    }

    @Override
    public void deleteMessageQueue(String name) {
        LOG.info("Deleting queue '" + getName() + "'");
        
        MessageQueueInfo info = queueEntityManager.get(name);
        if (info != null) {
            if (!info.getColumnFamilyBase().equals(this.managerName)) {
                // TODO:
            }
            
            queueEntityManager.delete(name);
            
            // TODO: Delete the shards for this queue
//            MutationBatch mb = keyspace.prepareMutationBatch();
//
//            for (MessageQueueShard partition : shardReaderPolicy.listShards()) {
//                mb.withRow(queueColumnFamily, partition.getName()).delete();
//            }
//
//            mb.withRow(queueColumnFamily, getName());
//
//            try {
//                mb.execute();
//            } catch (ConnectionException e) {
//                throw new MessageQueueException("Failed to clear messages from queue " + getName(), e);
//            }
        }
    }

    @Override
    public List<MessageQueueInfo> listMessageQueues() {
        return queueEntityManager.getAll();
    }

    @Override
    public void createStorage() throws MessageQueueException {
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(queueColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("key_validation_class",     "UTF8Type")
                        .put("comparator_type",          "CompositeType(BytesType, BytesType(reversed=true), TimeUUIDType, TimeUUIDType, BytesType)")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }
        });

        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(metadataColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("key_validation_class",     "UTF8Type")
                        .put("comparator_type",          "CompositeType(BytesType, UTF8Type)")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }
        });

        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(historyColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("default_validation_class", "UTF8Type")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }
        });
        
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                queueEntityManager.createStorage(null);
                return null;
            }
        });
        
        try {
            Properties props = keyspace.getKeyspaceProperties();
            LOG.info("Create storage: " + props.toString());
        }
        catch (Exception e) {
            
        }
    }

    @Override
    public void dropStorage() throws MessageQueueException {
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.dropColumnFamily(queueColumnFamily);
                return null;
            }            
        });
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.dropColumnFamily(metadataColumnFamily);
                return null;
            }            
        });
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.dropColumnFamily(historyColumnFamily);
                return null;
            }            
        });
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.dropColumnFamily(managerName + MessageQueueConstants.CF_INFO_SUFFIX);
                return null;
            }            
        });
    }
    
    private void changeSchema(Callable<Void> callable) throws MessageQueueException {
        for (int i = 0; i < 3; i++) {
            try {
                callable.call();
                try {
                    Thread.sleep(SCHEMA_CHANGE_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessageQueueException("Interrupted while trying to create column family for queuemanager " + getName(), ie);
                }
                return;
            } catch (SchemaDisagreementException e) {
                try {
                    Thread.sleep(SCHEMA_CHANGE_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessageQueueException("Interrupted while trying to create column family for queuemanager " + getName(), ie);
                }
            } catch (Exception e) {
                if (e.getMessage().contains("already exist"))
                    return;
                throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
            }
        }
    }
    
    public String getName() {
        return this.managerName;
    }
}
