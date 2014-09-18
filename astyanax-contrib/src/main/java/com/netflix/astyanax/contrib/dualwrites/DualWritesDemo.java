package com.netflix.astyanax.contrib.dualwrites;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class DualWritesDemo {

    final String cluster1 = "";
    final String ks1 = "";
    final String seed1 = "";
    
    final String cluster2 = "";
    final String ks2 = "";
    final String seed2 = "";
    
    final ColumnFamily<Integer, Long> CF_DUAL_WRITES = 
            ColumnFamily.newColumnFamily("dual_writes", IntegerSerializer.get(), LongSerializer.get(), StringSerializer.get());
    
    AstyanaxContext<Keyspace> ctx1;
    Keyspace keyspace1;
    
    AstyanaxContext<Keyspace> ctx2;
    Keyspace keyspace2;
    
    DualWritesKeyspace dualKeyspace = null;
    
    FailedWritesLogger logger = new LogBasedFailedWritesLogger();
    BestEffortSecondaryWriteStrategy execStrategy = new BestEffortSecondaryWriteStrategy(logger);

    public DualWritesDemo() {
        
        
        ctx1 = getKeyspaceContext(ks1, seed1);
        keyspace1 = ctx1.getClient();
        
        ctx2 = getKeyspaceContext(ks2, seed2);
        keyspace2 = ctx2.getClient();
        
        DualKeyspaceMetadata dualKeyspaceSetup = new DualKeyspaceMetadata(cluster1, ks1, cluster2, ks2);
        dualKeyspace = new DualWritesKeyspace(dualKeyspaceSetup, keyspace1, keyspace2, execStrategy);
    }
    
    public void run() throws Exception {

        ctx1 = getKeyspaceContext(ks1, seed1);
        keyspace1 = ctx1.getClient();
        ctx1.start();
        
        ctx2 = getKeyspaceContext(ks2, seed2);
        keyspace2 = ctx2.getClient();
        ctx2.start();
        
        runKS(keyspace1, 1, 0, 10);
        runKS(keyspace2, 2, 0, 10);
        
        logger.init();
        
        runKS(dualKeyspace, 3, 0, 10);
    }
    
    private void runKS(Keyspace ks, int rowKey, int start, int end) throws ConnectionException {
        
        MutationBatch mb = ks.prepareMutationBatch();
        for (long i=start; i<end; i++) {
            mb.withRow(CF_DUAL_WRITES, rowKey).putColumn(i, "foo");
        }
        mb.execute();
    }
    
    private AstyanaxContext<Keyspace> getKeyspaceContext(final String ks, final String seedHost) {
        AstyanaxContext<Keyspace> ctx = 
                new AstyanaxContext.Builder()
        .forKeyspace(ks)
        .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl("myCPConfig-" + ks)
                .setSeeds(seedHost)
                .setPort(7102))
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
                        .buildKeyspace(ThriftFamilyFactory.getInstance());

        return ctx;
    }

    public static void main(String[] args) {
     
        try {
            new DualWritesDemo().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
