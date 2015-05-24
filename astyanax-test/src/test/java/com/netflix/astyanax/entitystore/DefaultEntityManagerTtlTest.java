package com.netflix.astyanax.entitystore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.commons.lang.RandomStringUtils;
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
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class DefaultEntityManagerTtlTest {
	
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
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	@Entity
	@TTL(2)
	private static class TtlEntity {
	    @Id
	    private String id;
	    
	    @Column
	    private String column;
	    
	    public TtlEntity() {
	        
	    }

	    public String getId() {
	        return id;
	    }

	    public void setId(String id) {
	        this.id = id;
	    }

	    public String getColumn() {
	        return column;
	    }

	    public void setColumn(String column) {
	        this.column = column;
	    }
	    
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TtlEntity other = (TtlEntity) obj;
			if(id.equals(other.id) && column.equals(other.column))
				return true;
			else
				return false;
		}

	    @Override
	    public String toString() {
	        return "SimpleEntity [id=" + id + ", column=" + column + "]";
	    }
	}

	private TtlEntity createTtlEntity(String id) {
		TtlEntity e = new TtlEntity();
		e.setId(id);
		e.setColumn(RandomStringUtils.randomAlphanumeric(4));
		return e;
	}
	
	@Test
	public void testTtlClassAnnotation() throws Exception {
		final String id = "testTtlClassAnnotation";
		EntityManager<TtlEntity, String> entityPersister = new DefaultEntityManager.Builder<TtlEntity, String>()
				.withEntityType(TtlEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.build();
		TtlEntity origEntity = createTtlEntity(id);

		entityPersister.put(origEntity);

		// use low-level astyanax API to confirm the write
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			// test column number
			Assert.assertEquals(1, cl.size());
			// test column value
			Assert.assertEquals(origEntity.getColumn(), cl.getColumnByName("column").getStringValue());
			// custom ttl
			Assert.assertEquals(2, cl.getColumnByName("column").getTtl());
		}

		TtlEntity getEntity = entityPersister.get(id);
		Assert.assertEquals(id, getEntity.getId());
		Assert.assertEquals(origEntity, getEntity);

		// entity should expire after 3s since TTL is 2s in annotation
		Thread.sleep(1000 * 3);

		// use low-level astyanax API to confirm the TTL expiration
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
	
	@Test
	public void testConstructorTtlOverride() throws Exception {
		final String id = "testConstructorTtlOverride";
		EntityManager<TtlEntity, String> entityPersister = new DefaultEntityManager.Builder<TtlEntity, String>()
				.withEntityType(TtlEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.withTTL(5)
				.build();
		TtlEntity origEntity = createTtlEntity(id);

		entityPersister.put(origEntity);

		// use low-level astyanax API to confirm the write
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			// test column number
			Assert.assertEquals(1, cl.size());
			// test column value
			Assert.assertEquals(origEntity.getColumn(), cl.getColumnByName("column").getStringValue());
			// custom ttl
			Assert.assertEquals(5, cl.getColumnByName("column").getTtl());
		}

		TtlEntity getEntity = entityPersister.get(id);
		Assert.assertEquals(origEntity, getEntity);

		// entity should still be alive after 3s since TTL is overriden to 5s
		Thread.sleep(1000 * 3);
		
		getEntity = entityPersister.get(id);
		Assert.assertEquals(origEntity, getEntity);
		
		// entity should expire after 3s since 6s have passed with 5s TTL
		Thread.sleep(1000 * 3);

		// use low-level astyanax API to confirm the TTL expiration
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	@Entity
	private static class MethodTtlEntity {
	    @Id
	    private String id;
	    
	    @Column
	    private String column;
	    
	    public MethodTtlEntity() {
	        
	    }

		public String getId() {
	        return id;
	    }

	    public void setId(String id) {
	        this.id = id;
	    }

	    public String getColumn() {
	        return column;
	    }

	    public void setColumn(String column) {
	        this.column = column;
	    }
	    
	    @SuppressWarnings("unused")
		@TTL
	    public Integer getTTL() {
	    	return 2;
	    }
	    
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MethodTtlEntity other = (MethodTtlEntity) obj;
			if(id.equals(other.id) && column.equals(other.column))
				return true;
			else
				return false;
		}

	    @Override
	    public String toString() {
	        return "MethodTtlEntity [id=" + id + ", column=" + column + "]";
	    }
	}
	
	private MethodTtlEntity createMethodTtlEntity(String id) {
		MethodTtlEntity e = new MethodTtlEntity();
		e.setId(id);
		e.setColumn(RandomStringUtils.randomAlphanumeric(4));
		return e;
	}
	
	@Test
	public void testMethodTtlOverride() throws Exception {
		final String id = "testMethodTtlOverride";
		EntityManager<MethodTtlEntity, String> entityPersister = new DefaultEntityManager.Builder<MethodTtlEntity, String>()
				.withEntityType(MethodTtlEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(CF_SAMPLE_ENTITY)
				.withTTL(60) // constructor TTL value is 60s
				.build();
		MethodTtlEntity origEntity = createMethodTtlEntity(id);

		entityPersister.put(origEntity);

		// use low-level astyanax API to confirm the write
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			// test column number
			Assert.assertEquals(1, cl.size());
			// test column value
			Assert.assertEquals(origEntity.getColumn(), cl.getColumnByName("column").getStringValue());
			// custom ttl
			Assert.assertEquals(2, cl.getColumnByName("column").getTtl());
		}

		MethodTtlEntity getEntity = entityPersister.get(id);
		Assert.assertEquals(id, getEntity.getId());
		Assert.assertEquals(origEntity, getEntity);

		// entity should still be alive after 4s since TTL is overridden to 2s
		Thread.sleep(1000 * 4);

		// use low-level astyanax API to confirm the TTL expiration
		{
			ColumnList<String> cl = keyspace.prepareQuery(CF_SAMPLE_ENTITY).getKey(id).execute().getResult();
			Assert.assertEquals(0, cl.size());
		}
	}
	
}
