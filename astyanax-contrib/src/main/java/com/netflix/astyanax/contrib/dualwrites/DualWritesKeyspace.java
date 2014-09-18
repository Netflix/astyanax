package com.netflix.astyanax.contrib.dualwrites;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.ColumnMutation;
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
	public <K, C> OperationResult<Void> truncateColumnFamily(ColumnFamily<K, C> columnFamily) throws OperationException, ConnectionException {
		return getPrimaryKS().truncateColumnFamily(columnFamily);
	}

	@Override
	public OperationResult<Void> truncateColumnFamily(String columnFamily) throws ConnectionException {
		return getPrimaryKS().truncateColumnFamily(columnFamily);
	}

	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation) throws ConnectionException {
		return getPrimaryKS().testOperation(operation);
	}

	@Override
	public OperationResult<Void> testOperation(Operation<?, ?> operation, RetryPolicy retry) throws ConnectionException {
		return getPrimaryKS().testOperation(operation, retry);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> createColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) 
			throws ConnectionException {
		return getPrimaryKS().createColumnFamily(columnFamily, options);
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException {
		return getPrimaryKS().createColumnFamily(props);
	}

	@Override
	public OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException {
		return getPrimaryKS().createColumnFamily(options);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) 
			throws ConnectionException {
		return getPrimaryKS().updateColumnFamily(columnFamily, options);
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException {
		return getPrimaryKS().updateColumnFamily(props);
	}

	@Override
	public OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException {
		return getPrimaryKS().updateColumnFamily(options);
	}

	@Override
	public OperationResult<SchemaChangeResult> dropColumnFamily(String columnFamilyName) throws ConnectionException {
		return getPrimaryKS().dropColumnFamily(columnFamilyName);
	}

	@Override
	public <K, C> OperationResult<SchemaChangeResult> dropColumnFamily(ColumnFamily<K, C> columnFamily) throws ConnectionException {
		return getPrimaryKS().dropColumnFamily(columnFamily);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException {
		return getPrimaryKS().createKeyspace(options);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(Map<String, Object> options) throws ConnectionException {
		return getPrimaryKS().createKeyspaceIfNotExists(options);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Properties properties) throws ConnectionException {
		return getPrimaryKS().createKeyspace(properties);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(Properties properties) throws ConnectionException {
		return getPrimaryKS().createKeyspaceIfNotExists(properties);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options,Map<ColumnFamily, Map<String, Object>> cfs) 
			throws ConnectionException {
		return getPrimaryKS().createKeyspace(options, cfs);
	}

	@Override
	public OperationResult<SchemaChangeResult> createKeyspaceIfNotExists(Map<String, Object> options, Map<ColumnFamily, Map<String, Object>> cfs) 
			throws ConnectionException {
		return getPrimaryKS().createKeyspaceIfNotExists(options, cfs);
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException {
		return getPrimaryKS().updateKeyspace(options);
	}

	@Override
	public OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException {
		return getPrimaryKS().updateKeyspace(props);
	}

	@Override
	public OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException {
		return getPrimaryKS().dropKeyspace();
	}

	@Override
	public Map<String, List<String>> describeSchemaVersions() throws ConnectionException {
		return getPrimaryKS().describeSchemaVersions();
	}

	@Override
	public CqlStatement prepareCqlStatement() {
		return getPrimaryKS().prepareCqlStatement();
	}

	@Override
	public ConnectionPool<?> getConnectionPool() throws ConnectionException {
		return getPrimaryKS().getConnectionPool();
	}

	private class KeyspacePair {
		
        private final DualKeyspaceMetadata dualKeyspaceMetadata;
		private final AtomicReference<Keyspace> ksPrimary = new AtomicReference<Keyspace>(null);
		private final AtomicReference<Keyspace> ksSecondary = new AtomicReference<Keyspace>(null);
		
		private KeyspacePair(final DualKeyspaceMetadata dualKeyspaceSetup, final Keyspace pKS, final Keyspace sKS) {
            dualKeyspaceMetadata = dualKeyspaceSetup;
			ksPrimary.set(pKS);
			ksSecondary.set(sKS);
		}
		
        private Keyspace getPrimaryKS() {
            return ksPrimary.get();
        }

        private Keyspace getSecondaryKS() {
            return ksSecondary.get();
        }

        private DualKeyspaceMetadata getDualKSMetadata() {
            return dualKeyspaceMetadata;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((ksPrimary == null) ? 0 : ksPrimary.get().hashCode());
            result = prime * result + ((ksSecondary == null) ? 0 : ksSecondary.get().hashCode());
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
            
            equals &= (ksPrimary.get() == null) ?  (other.ksPrimary.get() == null) : (ksPrimary.get().equals(other.ksPrimary.get()));
            equals &= (ksSecondary.get() == null) ?  (other.ksSecondary.get() == null) : (ksSecondary.get().equals(other.ksSecondary.get()));
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
    public void flipPrimaryAndSecondary(final DualKeyspaceMetadata newDualKeyspaceSetup) {
        
        // Check that the expected state is actually reverse of what the destination state should be
        KeyspacePair pair = ksPair.get();
        DualKeyspaceMetadata currentKeyspaceSetup = pair.getDualKSMetadata();
        
        if (currentKeyspaceSetup.isReverse(newDualKeyspaceSetup)) {
            
            // flip
            KeyspacePair currentPair = ksPair.get();
            
            KeyspacePair newPair = 
                    new KeyspacePair(newDualKeyspaceSetup, currentPair.getSecondaryKS(), currentPair.getPrimaryKS());
            boolean success = ksPair.compareAndSet(currentPair, newPair);
            
            if (success) {
                Logger.info("Successfully flipped to new dual keyspace setup" + ksPair.get().getDualKSMetadata());
            }
            
        } else {
            Logger.error("Mismatch b/w expected primary andsecondary keyspaces. Will not FLIP to new keyspace setup: " + newDualKeyspaceSetup);
        }
    }
}
