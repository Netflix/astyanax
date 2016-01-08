package com.netflix.astyanax.contrib.dualwrites;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.model.CfSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.UnknownComparatorException;

/**
 * Main class that orchistrates all the dual writes. It wraps the 2 keyspaces (source and destination)
 * and appropriately forwards reads and writes to it as dictated by updates using the {@link DualWritesUpdateListener}
 * 
 * Note that if dual writes are enabled then the writes are sent to the {@link DualWritesMutationBatch} or {@link DualWritesColumnMutation}
 * classes appropriately. 
 * 
 * The reads are always served from the primary data source. 
 * 
 * @author poberai
 *
 */
public class DualWritesKeyspace implements Keyspace, DualWritesUpdateListener {

    private static final Logger Logger = LoggerFactory.getLogger(DualWritesKeyspace.class);
    
	private final AtomicReference<KeyspacePair> ksPair = new AtomicReference<KeyspacePair>(null);
    private final AtomicBoolean dualWritesEnabled = new AtomicBoolean(false);
	
    private final DualWritesStrategy executionStrategy;
    
	public DualWritesKeyspace(DualKeyspaceMetadata dualKeyspaceSetup,   
	                          Keyspace primaryKS, Keyspace secondaryKS,
	                          DualWritesStrategy execStrategy) {
		ksPair.set(new KeyspacePair(dualKeyspaceSetup, primaryKS, secondaryKS));
		executionStrategy = execStrategy;
	}
	
	private Keyspace getPrimaryKS() {
		return ksPair.get().getPrimaryKS();
	}
	
	public DualKeyspaceMetadata getDualKeyspaceMetadata() {
	    return ksPair.get().getDualKSMetadata();
	}

    @Override
	public AstyanaxConfiguration getConfig() {
		return getPrimaryKS().getConfig();
	}

	@Override
	public String getKeyspaceName() {
		return getPrimaryKS().getKeyspaceName();
	}

	@Override
	public Partitioner getPartitioner() throws ConnectionException {
		return getPrimaryKS().getPartitioner();
	}

	@Override
	public String describePartitioner() throws ConnectionException {
		return getPrimaryKS().describePartitioner();
	}

	@Override
	public List<TokenRange> describeRing() throws ConnectionException {
		return getPrimaryKS().describeRing();
	}

	@Override
	public List<TokenRange> describeRing(String dc) throws ConnectionException {
		return getPrimaryKS().describeRing(dc);
	}

	@Override
	public List<TokenRange> describeRing(String dc, String rack) throws ConnectionException {
		return getPrimaryKS().describeRing(dc, rack);
	}

	@Override
	public List<TokenRange> describeRing(boolean cached) throws ConnectionException {
		return getPrimaryKS().describeRing(cached);
	}

	@Override
	public KeyspaceDefinition describeKeyspace() throws ConnectionException {
		return getPrimaryKS().describeKeyspace();
	}

	@Override
	public Properties getKeyspaceProperties() throws ConnectionException {
		return getPrimaryKS().getKeyspaceProperties();
	}

	@Override
	public Properties getColumnFamilyProperties(String columnFamily) throws ConnectionException {
		return getPrimaryKS().getColumnFamilyProperties(columnFamily);
	}

	@Override
	public SerializerPackage getSerializerPackage(String cfName, boolean ignoreErrors) throws ConnectionException, UnknownComparatorException {
		return getPrimaryKS().getSerializerPackage(cfName, ignoreErrors);
	}

	@Override
	public MutationBatch prepareMutationBatch() {
	    if (dualWritesEnabled.get()) {
	        KeyspacePair pair = ksPair.get();
            return new DualWritesMutationBatch(
                                               pair.getDualKSMetadata(), 
                                               pair.getPrimaryKS().prepareMutationBatch(), 
                                               pair.getSecondaryKS().prepareMutationBatch(),
                                               executionStrategy);
	    } else {
	        return getPrimaryKS().prepareMutationBatch();
	    }
	}

	@Override
	public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
		return getPrimaryKS().prepareQuery(cf);
	}

	@Override
	public <K, C> ColumnMutation prepareColumnMutation(ColumnFamily<K, C> columnFamily, K rowKey, C column) {
	    KeyspacePair pair = ksPair.get();
	    if (dualWritesEnabled.get()) {
	        
	        WriteMetadata md = new WriteMetadata(pair.getDualKSMetadata(), columnFamily.getName(), rowKey.toString());
	        return new DualWritesColumnMutation(md,
	                                            pair.getPrimaryKS()  .prepareColumnMutation(columnFamily, rowKey, column), 
	                                            pair.getSecondaryKS().prepareColumnMutation(columnFamily, rowKey, column),
	                                            executionStrategy);
	    } else {
	        return pair.getPrimaryKS().prepareColumnMutation(columnFamily, rowKey, column);
	    }
	}

	@Override
	public <K, C> OperationResult<Void> truncateColumnFamily(final ColumnFamily<K, C> columnFamily) throws OperationException, ConnectionException {
        
	    return execDualKeyspaceOperation(new KeyspaceOperation<Void>() {
            @Override
            public OperationResult<Void> exec(Keyspace ks) throws ConnectionException {
                return ks.truncateColumnFamily(columnFamily);
            }
	    });
	}

	@Override
	public OperationResult<Void> truncateColumnFamily(final String columnFamily) throws ConnectionException {
	    return execDualKeyspaceOperation(new KeyspaceOperation<Void>() {
            @Override
            public OperationResult<Void> exec(Keyspace ks) throws ConnectionException {
                return ks.truncateColumnFamily(columnFamily);
            }
        });
	}

    @Override
    public List<String> describeSplits(String cfName, String startToken, String endToken, int keysPerSplit)
            throws ConnectionException {
        return null;
    }

    @Override
    public List<CfSplit> describeSplitsEx(String cfName, String startToken, String endToken, int keysPerSplit)
            throws ConnectionException {
        return null;
    }

    @Override
    public List<CfSplit> describeSplitsEx(String cfName, String startToken, String endToken, int keysPerSplit, ByteBuffer rowKey)
            throws ConnectionException {
        return null;
    }

    @Override
	public OperationResult<Void> testOperation(final Operation<?, ?> operation) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<Void>() {
            @Override
            public OperationResult<Void> exec(Keyspace ks) throws ConnectionException {
                return ks.testOperation(operation);
            }
        });
	}

	@Override
	public OperationResult<Void> testOperation(final Operation<?, ?> operation, RetryPolicy retry) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<Void>() {
            @Override
            public OperationResult<Void> exec(Keyspace ks) throws ConnectionException {
                return ks.testOperation(operation);
            }
        });
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createColumnFamily(columnFamily, options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(final Properties props) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createColumnFamily(props);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createColumnFamily(options);
            }
        });
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(final ColumnFamily<K, C> columnFamily, final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.updateColumnFamily(columnFamily, options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(final Properties props) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.updateColumnFamily(props);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.updateColumnFamily(options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(final String columnFamilyName) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.dropColumnFamily(columnFamilyName);
            }
        });
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(final ColumnFamily<K, C> columnFamily) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.dropColumnFamily(columnFamily);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspace(options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspaceIfNotExists(options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(final Properties properties) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspace(properties);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(final Properties properties) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspaceIfNotExists(properties);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(final Map<String, Object> options, final Map<ColumnFamily, Map<String, Object>> cfs)  throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspace(options, cfs);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(final Map<String, Object> options, final Map<ColumnFamily, Map<String, Object>> cfs) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.createKeyspaceIfNotExists(options, cfs);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(final Map<String, Object> options) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.updateKeyspace(options);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(final Properties props) throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.updateKeyspace(props);
            }
        });
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException {
        return execDualKeyspaceOperation(new KeyspaceOperation<SchemaChangeResult>() {
            @Override
            public OperationResult<SchemaChangeResult> exec(Keyspace ks) throws ConnectionException {
                return ks.dropKeyspace();
            }
        });
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		return getPrimaryKS().describeSchemaVersions();
	}

	@Override
	public CqlStatement prepareCqlStatement() {
	    KeyspacePair pair = ksPair.get();
        CqlStatement primaryStmt = pair.getPrimaryKS().prepareCqlStatement();
        CqlStatement secondaryStmt = pair.getSecondaryKS().prepareCqlStatement();
        return new DualWritesCqlStatement(primaryStmt, secondaryStmt, executionStrategy, pair.getDualKSMetadata());
	}

	@Override
	public ConnectionPool<?> getConnectionPool() throws ConnectionException {
		return getPrimaryKS().getConnectionPool();
	}

	private class KeyspacePair {
		
        private final DualKeyspaceMetadata dualKeyspaceMetadata;
		private final Keyspace ksPrimary;
		private final Keyspace ksSecondary;
		
		private KeyspacePair(final DualKeyspaceMetadata dualKeyspaceSetup, final Keyspace pKS, final Keyspace sKS) {
            dualKeyspaceMetadata = dualKeyspaceSetup;
			ksPrimary = pKS;
			ksSecondary = sKS;
		}
		
        private Keyspace getPrimaryKS() {
            return ksPrimary;
        }

        private Keyspace getSecondaryKS() {
            return ksSecondary;
        }

        private DualKeyspaceMetadata getDualKSMetadata() {
            return dualKeyspaceMetadata;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((ksPrimary == null) ? 0 : ksPrimary.hashCode());
            result = prime * result + ((ksSecondary == null) ? 0 : ksSecondary.hashCode());
            result = prime * result + ((dualKeyspaceMetadata == null) ? 0 : dualKeyspaceMetadata.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            
            KeyspacePair other = (KeyspacePair) obj;
            boolean equals = true;
            
            equals &= (ksPrimary == null) ?  (other.ksPrimary == null) : (ksPrimary.equals(other.ksPrimary));
            equals &= (ksSecondary == null) ?  (other.ksSecondary == null) : (ksSecondary.equals(other.ksSecondary));
            equals &= (dualKeyspaceMetadata == null) ?  (other.dualKeyspaceMetadata == null) : (dualKeyspaceMetadata.equals(other.dualKeyspaceMetadata));
            
            return equals;
        }
	}

    @Override
    public void dualWritesEnabled() {
        Logger.info("ENABLING dual writes for dual keyspace setup: " + ksPair.get().getDualKSMetadata());
        dualWritesEnabled.set(true);
    }

    @Override
    public void dualWritesDisabled() {
        Logger.info("DISABLING dual writes for dual keyspace setup: " + ksPair.get().getDualKSMetadata());
        dualWritesEnabled.set(false);
    }

    @Override
    public void flipPrimaryAndSecondary() {
        
        // Check that the expected state is actually reverse of what the destination state should be
        KeyspacePair currentPair = ksPair.get();

        DualKeyspaceMetadata currentKeyspaceSetup = currentPair.getDualKSMetadata();
        
        DualKeyspaceMetadata newDualKeyspaceSetup = 
                new DualKeyspaceMetadata(currentKeyspaceSetup.getSecondaryCluster(), currentKeyspaceSetup.getSecondaryKeyspaceName(),
                                         currentKeyspaceSetup.getPrimaryCluster(), currentKeyspaceSetup.getPrimaryKeyspaceName());

        KeyspacePair newPair = 
                new KeyspacePair(newDualKeyspaceSetup, currentPair.getSecondaryKS(), currentPair.getPrimaryKS());
        boolean success = ksPair.compareAndSet(currentPair, newPair);
            
        if (success) {
            Logger.info("Successfully flipped to new dual keyspace setup" + ksPair.get().getDualKSMetadata());
        } else {
            Logger.info("Could not flip keyspace pair: " + currentPair + " to new pair: " + newPair);
        }
    }
    
    private abstract class SimpleSyncExec<R> implements Execution<R> {
        @Override
        public ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException {
            throw new RuntimeException("executeAsync not implemented for SimpleSyncExec");
        }
    }
    
    private interface KeyspaceOperation<R> {
        OperationResult<R> exec(Keyspace ks) throws ConnectionException; 
    }
    
    private <R> OperationResult<R> execDualKeyspaceOperation(final KeyspaceOperation<R> ksOperation) throws ConnectionException {
        
        final KeyspacePair pair = ksPair.get();
                
        final Execution<R> exec1 = new SimpleSyncExec<R>() {
            @Override
            public OperationResult<R> execute() throws ConnectionException {
                return ksOperation.exec(pair.getPrimaryKS());
            }
        };
        final Execution<R> exec2 = new SimpleSyncExec<R>() {
            @Override
            public OperationResult<R> execute() throws ConnectionException {
                return ksOperation.exec(pair.getSecondaryKS());
            }
        };
        
        WriteMetadata writeMd = new WriteMetadata(pair.getDualKSMetadata(), null, null);

        return executionStrategy.wrapExecutions(exec1, exec2, Collections.singletonList(writeMd)).execute();
    }

}
