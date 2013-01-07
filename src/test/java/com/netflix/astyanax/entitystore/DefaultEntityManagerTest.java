package com.netflix.astyanax.entitystore;

import java.util.Date;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.entitystore.SampleEntity.Bar;
import com.netflix.astyanax.entitystore.SampleEntity.Bar.BarBar;
import com.netflix.astyanax.entitystore.SampleEntity.Foo;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class DefaultEntityManagerTest {

	private static Keyspace                  keyspace;
	private static AstyanaxContext<Keyspace> keyspaceContext;

	private static String TEST_CLUSTER_NAME  = "junit_cass_sandbox";
	private static String TEST_KEYSPACE_NAME = "EntityPersisterTestKeyspace";
	private static final String SEEDS = "localhost:9160";

	public static ColumnFamily<String, String> CF_SAMPLE_ENTITY = ColumnFamily.newColumnFamily(
			"SampleEntityColumnFamily", 
			StringSerializer.get(),
			StringSerializer.get());

	@BeforeClass
	public static void setup() throws Exception {

        SingletonEmbeddedCassandra.getInstance();

		Thread.sleep(1000 * 3);

		createKeyspace();
		
		Thread.sleep(1000 * 3);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (keyspaceContext != null)
			keyspaceContext.shutdown();

        Thread.sleep(1000 * 10);
	}

	private static void createKeyspace() throws Exception {
		keyspaceContext = new AstyanaxContext.Builder()
		.forCluster(TEST_CLUSTER_NAME)
		.forKeyspace(TEST_KEYSPACE_NAME)
		.withAstyanaxConfiguration(
				new AstyanaxConfigurationImpl()
				.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
				.setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
								+ "_" + TEST_KEYSPACE_NAME)
						.setSocketTimeout(30000)
						.setMaxTimeoutWhenExhausted(2000)
						.setMaxConnsPerHost(20)
						.setInitConnsPerHost(10)
						.setSeeds(SEEDS))
						.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
						.buildKeyspace(ThriftFamilyFactory.getInstance());

		keyspaceContext.start();

		keyspace = keyspaceContext.getEntity();

		try {
			keyspace.dropKeyspace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
				.put("strategy_options", ImmutableMap.<String, Object>builder()
						.put("replication_factor", "1")
						.build())
						.put("strategy_class",     "SimpleStrategy")
						.build()
				);

		keyspace.createColumnFamily(CF_SAMPLE_ENTITY, null);
	}

	private SampleEntity createRandomEntity(String id) {
		Random prng = new Random();
		SampleEntity entity = new SampleEntity();
		entity.setId(id);
		entity.setBooleanPrimitive(prng.nextBoolean());
		entity.setBooleanObject(prng.nextBoolean());
		entity.setBytePrimitive((byte)prng.nextInt(Byte.MAX_VALUE));
		entity.setByteObject((byte)prng.nextInt(Byte.MAX_VALUE));
		entity.setShortPrimitive((short)prng.nextInt(Short.MAX_VALUE));
		entity.setShortObject((short)prng.nextInt(Short.MAX_VALUE));
		entity.setIntPrimitive(prng.nextInt());
		entity.setIntObject(prng.nextInt());
		entity.setLongPrimitive(prng.nextLong());
		entity.setLongObject(prng.nextLong());
		entity.setFloatPrimitive(prng.nextFloat());
		entity.setFloatObject(prng.nextFloat());
		entity.setDoublePrimitive(prng.nextDouble());
		entity.setDoubleObject(prng.nextDouble());
		entity.setString(RandomStringUtils.randomAlphanumeric(16));
		entity.setByteArray(RandomStringUtils.randomAlphanumeric(16).getBytes(Charsets.UTF_8));
		entity.setDate(new Date());
		entity.setUuid(TimeUUIDUtils.getUniqueTimeUUIDinMicros());
		Foo foo = new Foo(prng.nextInt(), RandomStringUtils.randomAlphanumeric(4));
		entity.setFoo(foo);
		BarBar barbar = new BarBar();
		barbar.i = prng.nextInt();
		barbar.s = RandomStringUtils.randomAlphanumeric(4);
		Bar bar = new Bar();
		bar.i = prng.nextInt();
		bar.s = RandomStringUtils.randomAlphanumeric(4);
		bar.barbar = barbar;
		entity.setBar(bar);
		return entity;
	}

	@Test
	public void basicLifecycle() throws Exception {
		final String id = "basicLifecycle";
		EntityManager<SampleEntity, String> entityPersister = new DefaultEntityManager.Builder<SampleEntity, String>()
				.withEntityType(SampleEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.build();
		SampleEntity origEntity = createRandomEntity(id);

		entityPersister.put(origEntity);

		// use low-level astyanax API to confirm the write
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			
			// 19 simple columns
			// 2 one-level-deep nested columns from Bar
			// 2 two-level-deep nested columns from BarBar
			Assert.assertEquals(23, cl.size());
			
			// simple columns
			Assert.assertEquals(origEntity.getString(), cl.getColumnByName("string").getStringValue());
			Assert.assertArrayEquals(origEntity.getByteArray(), cl.getColumnByName("byte_array").getByteArrayValue());
			
			// custom ttl
			Assert.assertEquals(123456, cl.getColumnByName("byte_array").getTtl());
			
			//  nested fields
			Assert.assertEquals(origEntity.getBar().i, cl.getColumnByName("bar.i").getIntegerValue());
			Assert.assertEquals(origEntity.getBar().s, cl.getColumnByName("bar.s").getStringValue());
			Assert.assertEquals(origEntity.getBar().barbar.i, cl.getColumnByName("bar.barbar.i").getIntegerValue());
			Assert.assertEquals(origEntity.getBar().barbar.s, cl.getColumnByName("bar.barbar.s").getStringValue());
		}

		SampleEntity getEntity = entityPersister.get(id);
		Assert.assertEquals(origEntity, getEntity);

		entityPersister.delete(id);

		// use low-level astyanax API to confirm the delete
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
}
