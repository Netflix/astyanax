package com.netflix.astyanax.cql.test.entitymapper;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.cql.test.KeyspaceTests;
import com.netflix.astyanax.cql.test.entitymapper.EntityMapperTests.SampleTestCompositeEntity.InnerEntity;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public class EntityMapperTests extends KeyspaceTests {
	
    private static ColumnFamily<String, String> CF_SAMPLE_TEST_ENTITY = ColumnFamily
            .newColumnFamily(
                    "sampletestentity", 
                    StringSerializer.get(),
                    StringSerializer.get());

	private static EntityManager<SampleTestEntity, String> entityManager;
	private static EntityManager<SampleTestCompositeEntity, String> compositeEntityManager;

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_SAMPLE_TEST_ENTITY, null);
		
    	CF_SAMPLE_TEST_ENTITY.describe(keyspace);
    	
    	entityManager = 
        		new DefaultEntityManager.Builder<SampleTestEntity, String>()
        		.withEntityType(SampleTestEntity.class)
        		.withKeyspace(keyspace)
        		.withColumnFamily(CF_SAMPLE_TEST_ENTITY)
        		.build();

    	compositeEntityManager = 
        		new DefaultEntityManager.Builder<SampleTestCompositeEntity, String>()
        		.withEntityType(SampleTestCompositeEntity.class)
        		.withKeyspace(keyspace)
        		.withColumnFamily(CF_SAMPLE_TEST_ENTITY)
        		.build();
    }

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_SAMPLE_TEST_ENTITY);
	}
    
    
    @Test
    public void testSimpleEntityCRUD() throws Exception {
    	
    	final String ID = "testSimpleEntityCRUD";
    	
    	final SampleTestEntity testEntity = new SampleTestEntity();
    	
    	testEntity.id = ID;
    	testEntity.testInt = 1;
    	testEntity.testLong = 2L;
    	testEntity.testString = "testString1";
    	testEntity.testDouble = 3.0;
    	testEntity.testFloat = 4.0f; 
    	testEntity.testBoolean = true;
    	
    	// PUT
    	entityManager.put(testEntity);
    	
    	// GET
    	SampleTestEntity getEntity = entityManager.get(ID);
    	Assert.assertNotNull(getEntity);
    	Assert.assertTrue(testEntity.equals(getEntity));
    	
    	// DELETE
    	entityManager.delete(ID);
    	getEntity = entityManager.get(ID);
    	Assert.assertNull(getEntity);
    }

    @Test
    public void testSimpleEntityList() throws Exception {
    	
    	List<SampleTestEntity> entities = new ArrayList<SampleTestEntity>();
    	List<String> ids = new ArrayList<String>();
    	
    	int entityCount = 11;
    	
    	for (int i=0; i<entityCount; i++) {
    		
        	SampleTestEntity testEntity = new SampleTestEntity();
        	
        	testEntity.id = "id" + i;
        	testEntity.testInt = i;
        	testEntity.testLong = i;
        	testEntity.testString = "testString" + i;
        	testEntity.testDouble = i;
        	testEntity.testFloat = i; 
        	testEntity.testBoolean = true;
        	
        	entities.add(testEntity);
        	ids.add("id" + i);
    	}
    	
    	// PUT COLLECTION
    	entityManager.put(entities);
    	
    	// GET
    	List<SampleTestEntity> getEntities = entityManager.get(ids);
    	Assert.assertTrue(entityCount == getEntities.size());
    	
    	int count = 0;
    	for (SampleTestEntity e : getEntities) {
    		Assert.assertTrue(count == e.testInt);
    		Assert.assertTrue(count == e.testLong);
    		Assert.assertTrue(("testString" + count).equals(e.testString));
    		count++;
    	}
    	
    	// DELETE
    	entityManager.delete(ids);
    	
    	// GET AFTER DELETE
    	getEntities = entityManager.get(ids);
    	Assert.assertTrue(0 == getEntities.size());
    }

    @Test
    public void testGetAll() throws Exception {
    	
    	List<SampleTestEntity> entities = new ArrayList<SampleTestEntity>();
    	List<String> ids = new ArrayList<String>();
    	
    	int entityCount = 11;
    	
    	for (int i=0; i<entityCount; i++) {
    		
        	SampleTestEntity testEntity = new SampleTestEntity();
        	
        	testEntity.id = "id" + i;
        	testEntity.testInt = i;
        	testEntity.testLong = i;
        	testEntity.testString = "testString" + i;
        	testEntity.testDouble = i;
        	testEntity.testFloat = i; 
        	testEntity.testBoolean = true;
        	
        	entities.add(testEntity);
        	ids.add("id" + i);
    	}
    	
    	// PUT COLLECTION
    	entityManager.put(entities);
    	
    	List<SampleTestEntity> getEntities = entityManager.getAll();
    	Assert.assertTrue(entityCount == getEntities.size());
    	for (SampleTestEntity e : getEntities) {
    		String id = e.id;
    		int i = Integer.parseInt(id.substring("id".length()));
    		Assert.assertTrue(i == e.testInt);
    		Assert.assertTrue(i == e.testLong);
    		Assert.assertTrue(("testString" + i).equals(e.testString));
    	}
    	
    	// DELETE
    	entityManager.delete(ids);
    	// GET AFTER DELETE
    	getEntities = entityManager.getAll();
    	Assert.assertTrue(0 == getEntities.size());
    }

    @Test
    public void testCompositeEntityCRUD() throws Exception {
    	
    	final String ID = "testCompositeEntityCRUD";

    	final SampleTestCompositeEntity testEntity = new SampleTestCompositeEntity();
    	
    	testEntity.id = ID;
    	testEntity.testInt = 1;
    	testEntity.testLong = 2L;
    	testEntity.testString = "testString1";
    	testEntity.testDouble = 3.0;
    	testEntity.testFloat = 4.0f; 
    	testEntity.testBoolean = true;
    	
    	testEntity.inner = new InnerEntity();
    	testEntity.inner.testInnerInt = 11;
    	testEntity.inner.testInnerLong = 22L;
    	testEntity.inner.testInnerString = "testInnserString1";
    	
    	// PUT
    	compositeEntityManager.put(testEntity);
    	
    	// GET
    	SampleTestCompositeEntity getEntity = compositeEntityManager.get(ID);
    	System.out.println(getEntity);
    	Assert.assertNotNull(getEntity);
    	Assert.assertTrue(testEntity.equals(getEntity));
    	
    	// DELETE
    	entityManager.delete(ID);
    	getEntity = compositeEntityManager.get(ID);
    	Assert.assertNull(getEntity);
    }

    @Entity
    public static class SampleTestEntity {
    	
    	@Id
    	private String id;
    	
    	@Column(name="integer")
    	private int testInt; 
    	@Column(name="long")
    	private long testLong;
    	@Column(name="string")
    	private String testString; 
    	@Column(name="float")
    	private float testFloat;
    	@Column(name="double")
    	private double testDouble;
    	@Column(name="boolean")
    	private boolean testBoolean;
    	
    	public SampleTestEntity() {
    		
    	}
    	
		@Override
		public String toString() {
			return "SampleTestEntity [\nid=" + id + "\ntestInt=" + testInt
					+ "\ntestLong=" + testLong + "\ntestString=" + testString
					+ "\ntestFloat=" + testFloat + "\ntestDouble=" + testDouble
					+ "\ntestBoolean=" + testBoolean + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + (testBoolean ? 1231 : 1237);
			long temp;
			temp = Double.doubleToLongBits(testDouble);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime * result + Float.floatToIntBits(testFloat);
			result = prime * result + testInt;
			result = prime * result + (int) (testLong ^ (testLong >>> 32));
			result = prime * result + ((testString == null) ? 0 : testString.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			
			SampleTestEntity other = (SampleTestEntity) obj;
			boolean equal = true;

			equal &=  (id != null) ? id.equals(other.id) : other.id == null;
			equal &=  testInt == other.testInt;
			equal &=  testLong == other.testLong;
			equal &=  testBoolean == other.testBoolean;
			equal &=  (testString != null) ? testString.equals(other.testString) : other.testString == null;
			equal &= (Double.doubleToLongBits(testDouble) == Double.doubleToLongBits(other.testDouble));
			equal &= (Float.floatToIntBits(testFloat) == Float.floatToIntBits(other.testFloat));
			
			return equal;
		}
    }
    
    @Entity
    public static class SampleTestCompositeEntity {
    	
    	@Id
    	private String id;
    	
    	@Column(name="integer")
    	private int testInt; 
    	@Column(name="long")
    	private long testLong;
    	@Column(name="string")
    	private String testString; 
    	@Column(name="float")
    	private float testFloat;
    	@Column(name="double")
    	private double testDouble;
    	@Column(name="boolean")
    	private boolean testBoolean;
    	
    	@Entity
    	public static class InnerEntity {
    		
        	@Column(name="inner_integer")
        	private int testInnerInt; 
        	@Column(name="inner_long")
        	private long testInnerLong;
        	@Column(name="inner_string")
        	private String testInnerString; 
        	
    		@Override
    		public String toString() {
    			return "InnerEntity [\ninnerInt="  + testInnerInt
    					+ "\ninnerLong=" + testInnerLong + "\ninnerString=" + testInnerString + "]";
    		}

    		@Override
    		public int hashCode() {
    			final int prime = 31;
    			int result = 1;
    			result = prime * result + testInnerInt;
    			result = prime * result + (int) (testInnerLong ^ (testInnerLong >>> 32));
    			result = prime * result + ((testInnerString == null) ? 0 : testInnerString.hashCode());
    			return result;
    		}

    		@Override
    		public boolean equals(Object obj) {
    			
    			if (this == obj) return true;
    			if (obj == null) return false;
    			if (getClass() != obj.getClass()) return false;
    			
    			InnerEntity other = (InnerEntity) obj;
    			boolean equal = true;
    			equal &=  testInnerInt == other.testInnerInt;
    			equal &=  testInnerLong == other.testInnerLong;
    			equal &=  (testInnerString != null) ? testInnerString.equals(other.testInnerString) : other.testInnerString == null;
    			return equal;
    		}

    	}
    	
    	@Column(name="inner")
    	private InnerEntity inner; 
    	
    	public SampleTestCompositeEntity() {
    		
    	}
    	
		@Override
		public String toString() {
			return "SampleTestEntity [\nid=" + id + "\ntestInt=" + testInt
					+ "\ntestLong=" + testLong + "\ntestString=" + testString
					+ "\ntestFloat=" + testFloat + "\ntestDouble=" + testDouble
					+ "\ntestBoolean=" + testBoolean 
					+ "\ninner = " + inner.toString() + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + (testBoolean ? 1231 : 1237);
			long temp;
			temp = Double.doubleToLongBits(testDouble);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime * result + Float.floatToIntBits(testFloat);
			result = prime * result + testInt;
			result = prime * result + (int) (testLong ^ (testLong >>> 32));
			result = prime * result + ((testString == null) ? 0 : testString.hashCode());
			result = prime * result + ((inner == null) ? 0 : inner.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			
			SampleTestCompositeEntity other = (SampleTestCompositeEntity) obj;
			boolean equal = true;

			equal &=  (id != null) ? id.equals(other.id) : other.id == null;
			equal &=  testInt == other.testInt;
			equal &=  testLong == other.testLong;
			equal &=  testBoolean == other.testBoolean;
			equal &=  (testString != null) ? testString.equals(other.testString) : other.testString == null;
			equal &= (Double.doubleToLongBits(testDouble) == Double.doubleToLongBits(other.testDouble));
			equal &= (Float.floatToIntBits(testFloat) == Float.floatToIntBits(other.testFloat));
			equal &=  (inner != null) ? inner.equals(other.inner) : other.inner == null;
			
			return equal;
		}
    }
}
