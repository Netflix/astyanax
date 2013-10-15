package com.netflix.astyanax.entitystore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.entitystore.NullableEntity.AllMandatoryNestedEntity;
import com.netflix.astyanax.entitystore.NullableEntity.AllOptionalNestedEntity;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class DefaultEntityManagerNullableTest {

	private static Keyspace                  keyspace;
	private static AstyanaxContext<Keyspace> keyspaceContext;

	private static String TEST_CLUSTER_NAME  = "junit_cass_sandbox";
	private static String TEST_KEYSPACE_NAME = "EntityPersisterTestKeyspace";
	private static final String SEEDS = "localhost:9160";

	public static ColumnFamily<String, String> CF_SAMPLE_ENTITY = ColumnFamily.newColumnFamily(
			"SampleEntityColumnFamily", 
			StringSerializer.get(),
			StringSerializer.get());

	public static ColumnFamily<String, String> CF_SIMPLE_ENTITY = ColumnFamily.newColumnFamily(
			"SimpleEntityColumnFamily", 
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
				.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
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
		keyspace.createColumnFamily(CF_SIMPLE_ENTITY, null);
	}
	
	private NullableEntity createNullableEntity(final String id) {
		NullableEntity entity = new NullableEntity();
		
		entity.setId(id);
		entity.setNotnullable("notnullable");
		entity.setNullable("nullable");
		
		AllOptionalNestedEntity notnullableAllOptionalNestedEntity = new AllOptionalNestedEntity();
		notnullableAllOptionalNestedEntity.setNullable("notnullableAllOptionalNestedEntity");
		entity.setNotnullableAllOptionalNestedEntity(notnullableAllOptionalNestedEntity);
		
		AllOptionalNestedEntity nullableAllOptionalNestedEntity = new AllOptionalNestedEntity();
		nullableAllOptionalNestedEntity.setNullable("nullableAllOptionalNestedEntity");
		entity.setNullableAllOptionalNestedEntity(nullableAllOptionalNestedEntity);
		
		AllMandatoryNestedEntity notnullableAllMandatoryNestedEntity = new AllMandatoryNestedEntity();
		notnullableAllMandatoryNestedEntity.setNotnullable("notnullableAllMandatoryNestedEntity");
		entity.setNotnullableAllMandatoryNestedEntity(notnullableAllMandatoryNestedEntity);
		
		AllMandatoryNestedEntity nullableAllMandatoryNestedEntity = new AllMandatoryNestedEntity();
		nullableAllMandatoryNestedEntity.setNotnullable("nullableAllMandatoryNestedEntity");
		entity.setNullableAllMandatoryNestedEntity(nullableAllMandatoryNestedEntity);
		
		return entity;
	}
	
	@Test
	public void nullableColumn() throws Exception {
		final String id = "nullableColumn";
		EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
				.withEntityType(NullableEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.build();

		NullableEntity origEntity = createNullableEntity(id);
		origEntity.setNullable(null);
		
		entityPersister.put(origEntity);
		
		// use low-level astyanax API to confirm the null column
		// is not written as empty column
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			// test column number
			Assert.assertEquals(5, cl.size());
			// assert non-existent
			Assert.assertNull(cl.getColumnByName("nullable"));
			// test column value
			Assert.assertEquals(origEntity.getNotnullable(), cl.getColumnByName("notnullable").getStringValue());
		}

		NullableEntity getEntity = entityPersister.get(id);
		assertNull(getEntity.getNullable());
		assertEquals(origEntity, getEntity);
		
		entityPersister.delete(id);

		// use low-level astyanax API to confirm the delete
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
	
	@Test
	public void expectNullColumnException() throws Exception {
		final String id = "expectNullColumnException";
		try {
			EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
					.withEntityType(NullableEntity.class)
					.withKeyspace(keyspace)
					.withColumnFamily(CF_SAMPLE_ENTITY)
					.build();
			NullableEntity origEntity = createNullableEntity(id);
			origEntity.setNotnullable(null);
			entityPersister.put(origEntity);
		} catch(PersistenceException e) {
			// catch expected exception and verify the cause
			Throwable rootCause = ExceptionUtils.getRootCause(e);
			assertEquals(IllegalArgumentException.class, rootCause.getClass());
			assertEquals("cannot write non-nullable column with null value: notnullable", rootCause.getMessage());
		}
	}
	
	@Test
	public void nullableNestedColumn() throws Exception {
		final String id = "nullableNestedColumn";
		EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
				.withEntityType(NullableEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.build();

		NullableEntity origEntity = createNullableEntity(id);
		origEntity.setNullableAllOptionalNestedEntity(null);
		origEntity.getNotnullableAllOptionalNestedEntity().setNullable(null);
		
		entityPersister.put(origEntity);
		
		// use low-level astyanax API to confirm the null column
		// is not written as empty column
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			// test column number
			Assert.assertEquals(4, cl.size());
			// assert non-existent
			Assert.assertNull(cl.getColumnByName("nullableAllOptionalNestedEntity.nullable"));
			Assert.assertNull(cl.getColumnByName("notnullableAllOptionalNestedEntity.nullable"));
			// test column value
			Assert.assertEquals(origEntity.getNotnullable(), cl.getColumnByName("notnullable").getStringValue());
			Assert.assertEquals(origEntity.getNullable(), cl.getColumnByName("nullable").getStringValue());
			Assert.assertEquals(origEntity.getNotnullableAllMandatoryNestedEntity().getNotnullable(), cl.getColumnByName("notnullableAllMandatoryNestedEntity.notnullable").getStringValue());
			Assert.assertEquals(origEntity.getNullableAllMandatoryNestedEntity().getNotnullable(), cl.getColumnByName("nullableAllMandatoryNestedEntity.notnullable").getStringValue());
		}

		NullableEntity getEntity = entityPersister.get(id);
		Assert.assertNull(getEntity.getNullableAllOptionalNestedEntity());
		// note this is special. it is NOT null
//		Assert.assertNotNull(getEntity.getNotnullableAllOptionalNestedEntity());
//		Assert.assertNull(getEntity.getNotnullableAllOptionalNestedEntity().getNullable());
//		Assert.assertEquals(origEntity, getEntity);
		
		entityPersister.delete(id);

		// use low-level astyanax API to confirm the delete
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
	
	@Test
	public void expectNullColumnExceptionNotnullableAllOptionalNestedEntity() throws Exception {
		final String id = "expectNullColumnExceptionNotnullableAllOptionalNestedEntity";
		try {
			EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
					.withEntityType(NullableEntity.class)
					.withKeyspace(keyspace)
					.withColumnFamily(CF_SAMPLE_ENTITY)
					.build();
			
			NullableEntity origEntity = createNullableEntity(id);
			origEntity.setNotnullableAllOptionalNestedEntity(null);

			entityPersister.put(origEntity);
		} catch(PersistenceException e) {
			// catch expected exception and verify the cause
			Throwable rootCause = ExceptionUtils.getRootCause(e);
			assertEquals(IllegalArgumentException.class, rootCause.getClass());
			assertEquals("cannot write non-nullable column with null value: notnullableAllOptionalNestedEntity", rootCause.getMessage());
		}
	}
	
	@Test
	public void expectNullColumnExceptionNotnullableAllMandatoryNestedEntity() throws Exception {
		final String id = "expectNullColumnExceptionNotnullableAllMandatoryNestedEntity";
		try {
			EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
					.withEntityType(NullableEntity.class)
					.withKeyspace(keyspace)
					.withColumnFamily(CF_SAMPLE_ENTITY)
					.build();
			
			NullableEntity origEntity = createNullableEntity(id);
			origEntity.setNotnullableAllMandatoryNestedEntity(null);

			entityPersister.put(origEntity);
		} catch(PersistenceException e) {
			// catch expected exception and verify the cause
			Throwable rootCause = ExceptionUtils.getRootCause(e);
			assertEquals(IllegalArgumentException.class, rootCause.getClass());
			assertEquals("cannot write non-nullable column with null value: notnullableAllMandatoryNestedEntity", rootCause.getMessage());
		}
	}
	
	@Test
	public void expectNestedNullColumnExceptionNullableAllMandatoryNestedEntityNullChild() throws Exception {
		final String id = "expectNestedNullColumnException";
		try {
			EntityManager<NullableEntity, String> entityPersister = new DefaultEntityManager.Builder<NullableEntity, String>()
					.withEntityType(NullableEntity.class)
					.withKeyspace(keyspace)
					.withColumnFamily(CF_SAMPLE_ENTITY)
					.build();
			
			NullableEntity origEntity = createNullableEntity(id);
			origEntity.getNullableAllMandatoryNestedEntity().setNotnullable(null);

			entityPersister.put(origEntity);
		} catch(PersistenceException e) {
			// catch expected exception and verify the cause
			Throwable rootCause = ExceptionUtils.getRootCause(e);
			assertEquals(IllegalArgumentException.class, rootCause.getClass());
			assertEquals("cannot write non-nullable column with null value: notnullable", rootCause.getMessage());
		}
	}

}
