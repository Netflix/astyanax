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
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class DualWritesDemo {

    final String cluster1 = "cass_dualwrites_source";
    final String ks1 = "dualwritessrc";
    final String seed1 = "";

    final String cluster2 = "cass_dualwrites_dest";
    final String ks2 = "dualwritessrc";
    final String seed2 = "";

    final ColumnFamily<Integer, Long> CF_DUAL_WRITES = 
            ColumnFamily.newColumnFamily("foobar", IntegerSerializer.get(), LongSerializer.get(), StringSerializer.get());

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

    }

    public void run() throws Exception {

        try {
            ctx1 = getKeyspaceContext(ks1, seed1);
            keyspace1 = ctx1.getClient();
            ctx1.start();

            ctx2 = getKeyspaceContext(ks2, seed2);
            keyspace2 = ctx2.getClient();
            ctx2.start();

            Thread.sleep(100);

            logger.init();

            DualKeyspaceMetadata dualKeyspaceSetup = new DualKeyspaceMetadata(cluster1, ks1, cluster2, ks2);
            dualKeyspace = new DualWritesKeyspace(dualKeyspaceSetup, keyspace1, keyspace2, execStrategy);

            addRowToKS(dualKeyspace, 1, 0, 10);
            verifyPresent(keyspace1, 1);
            verifyNotPresent(keyspace2, 1);


            dualKeyspace.dualWritesEnabled();
            addRowToKS(dualKeyspace, 2, 0, 10);

            verifyPresent(keyspace1, 2);
            verifyPresent(keyspace2, 2);

            dualKeyspace.dualWritesDisabled();
            addRowToKS(dualKeyspace, 3, 0, 10);

            verifyPresent(keyspace1, 3);
            verifyNotPresent(keyspace2, 3);

            dualKeyspace.flipPrimaryAndSecondary();

            addRowToKS(dualKeyspace, 4, 0, 10);

            verifyNotPresent(keyspace1, 4);
            verifyPresent(keyspace2, 4);

            dualKeyspace.dualWritesEnabled();
            addRowToKS(dualKeyspace, 5, 0, 10);

            verifyPresent(keyspace1, 5);
            verifyPresent(keyspace2, 5);

            dualKeyspace.dualWritesDisabled();
            addRowToKS(dualKeyspace, 6, 0, 10);

            verifyNotPresent(keyspace1, 6);
            verifyPresent(keyspace2, 6);

            dualKeyspace.flipPrimaryAndSecondary();
            addRowToKS(dualKeyspace, 7, 0, 10);

            verifyPresent(keyspace1, 7);
            verifyNotPresent(keyspace2, 7);

            dualKeyspace.dualWritesEnabled();
            addRowToKS(dualKeyspace, 8, 0, 10);

            verifyPresent(keyspace1, 8);
            verifyPresent(keyspace2, 8);

            deleteRowFromKS(dualKeyspace, 1,2,3,4,5,6,7,8);

            verifyNotPresent(keyspace1, 1,2,3,4,5,6,7,8);
            verifyNotPresent(keyspace2, 1,2,3,4,5,6,7,8);

        } finally {
            if (ctx1 != null) {
                ctx1.shutdown();
            }
            if (ctx2 != null) {
                ctx2.shutdown();
            }
        }
    }

    private void addRowToKS(Keyspace ks, int rowKey, int start, int end) throws ConnectionException {

        MutationBatch mb = ks.prepareMutationBatch();
        for (long i=start; i<end; i++) {
            mb.withRow(CF_DUAL_WRITES, rowKey).putColumn(i, "foo");
        }
        mb.execute();
    }

    private void deleteRowFromKS(Keyspace ks, int ... rowKeys) throws ConnectionException {

        MutationBatch mb = ks.prepareMutationBatch();
        for (int rowKey : rowKeys) {
            mb.withRow(CF_DUAL_WRITES, rowKey).delete();
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
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
                        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
                        .buildKeyspace(ThriftFamilyFactory.getInstance());

        return ctx;
    }

    private void verifyPresent(Keyspace ks, int rowKey) throws ConnectionException {

        ColumnList<Long> result = ks.prepareQuery(CF_DUAL_WRITES).getRow(rowKey).execute().getResult();
        if (result.isEmpty()) {
            throw new RuntimeException("Row: " + rowKey + " missing from keysapce: " + ks.getKeyspaceName());
        } else {
            System.out.println("Verified Row: " + rowKey + " present in ks: " + ks.getKeyspaceName());
        }
    }

    private void verifyNotPresent(Keyspace ks, int rowKey) throws ConnectionException {

        ColumnList<Long> result = ks.prepareQuery(CF_DUAL_WRITES).getRow(rowKey).execute().getResult();
        if (!result.isEmpty()) {
            throw new RuntimeException("Row: " + rowKey + " present in keysapce: " + ks.getKeyspaceName());
        } else {
            System.out.println("Verified Row: " + rowKey + " NOT present in ks: " + ks.getKeyspaceName());
        }
    }

    private void verifyNotPresent(Keyspace ks, int ... rowKeys) throws ConnectionException {
        for (int rowKey : rowKeys) {
            verifyNotPresent(ks, rowKey);
        }
    }

    public static void main(String[] args) {

        try {
            new DualWritesDemo().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
