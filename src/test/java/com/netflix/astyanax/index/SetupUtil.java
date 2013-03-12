package com.netflix.astyanax.index;

import java.io.IOException;
import java.util.Date;

import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.test.EmbeddedCassandra;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class SetupUtil {

	public static long SERVER_START_TIME = 3000;
	public static String DEF_KEYSPACE_NAME = "icrskeyspace";

	
	static EmbeddedCassandra cassandra;
	//to be removed
	@Deprecated
	public static synchronized EmbeddedCassandra startCassandra() throws IOException, TTransportException,InterruptedException {
		if (cassandra == null) {
			cassandra = new EmbeddedCassandra();
			cassandra.start();
			Thread.sleep(SERVER_START_TIME);
		}
		return cassandra;
		
	}
	public static synchronized void stopCassandra() {
		//can't stop it as other tests may be relying on it.
		//it will be cleaned on vm exit
		//if (cassandra != null)
		//cassandra.stop();
	}
	
	public static AstyanaxContext<Keyspace> initKeySpace() throws ConnectionException   {
		return initKeySpace(DEF_KEYSPACE_NAME);

	}

	public static AstyanaxContext<Keyspace> initKeySpace(String keyspaceName) throws ConnectionException {
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.forKeyspace(keyspaceName)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds("127.0.0.1:9160"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		Keyspace keyspace = context.getEntity();
		try {
			keyspace.dropKeyspace();
		} catch (Exception e) {
			System.out.println("Exception dropping keyspace: " + e.getMessage());
			//e.printStackTrace();
		}

		keyspace.createKeyspace(ImmutableMap
				.<String, Object> builder()
				.put("strategy_options",
						ImmutableMap.<String, Object> builder()
								.put("replication_factor", "1").build())
				.put("strategy_class", "SimpleStrategy").build());

		return context;

	}

	public static void indexCFSetup(Keyspace keyspace) throws ConnectionException  {
		ColumnFamily<byte[], byte[]> index_cf = ColumnFamily.newColumnFamily(
				"index_cf", BytesArraySerializer.get(),
				BytesArraySerializer.get());
		
		try {
			keyspace.dropColumnFamily(index_cf);
		}catch (Exception e) {
			System.out.println("Exception dropping index: " + index_cf);
			//e.printStackTrace();
		}
		keyspace.createColumnFamily(index_cf, ImmutableMap
				.<String, Object> builder().put("caching", "ALL").build());

	}

	public static IndexCoordination initIndexCoordinator() {

		IndexCoordination indexCoordination = IndexCoordinationFactory
				.getIndexContext();

		IndexMetadata<String, String> metaData = new IndexMetadata<String, String>(
				"device_service", "pin", String.class);
		indexCoordination.addIndexMetaData(metaData);

		return indexCoordination;

	}

	public static void devSrvDataSetup(Keyspace keyspace) throws Exception {

		// device_service CF
		ColumnFamily<String, String> DEV_SERV_CF = ColumnFamily
				.newColumnFamily("device_service", StringSerializer.get(),
						StringSerializer.get());

		keyspace.createColumnFamily(
				DEV_SERV_CF,
				ImmutableMap.<String, Object> builder()
						.put("default_validation_class", "BytesType")
						.put("key_validation_class", "UTF8Type")
						.put("comparator_type", "UTF8Type").build());

		// some data:
		MutationBatch m = keyspace.prepareMutationBatch();

		for (int i = 0; i < 20; i++) {
			String pin = "10099888" + i;

			m.withRow(DEV_SERV_CF, pin + ":srv_1").putColumn("pin", pin)
					.putColumn("body", "body___ " + pin)
					.putColumn("expiry_date", new Date())
					.putColumn("srv_id", "srv_1").putColumn("type", "type_1")
					.putColumn("class", "class_" + i);

			m.withRow(DEV_SERV_CF, pin + ":srv_2").putColumn("pin", pin)
					.putColumn("body", "body___ " + pin)
					.putColumn("expiry_date", new Date())
					.putColumn("srv_id", "srv_2").putColumn("type", "type_2")
					.putColumn("class", "class_" + i);

		}
		m.execute();
	}

}
