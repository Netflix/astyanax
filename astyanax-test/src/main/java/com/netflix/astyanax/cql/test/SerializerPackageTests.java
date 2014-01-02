package com.netflix.astyanax.cql.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.SpecificCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class SerializerPackageTests extends KeyspaceTests {

	private static final Logger LOG = Logger.getLogger(SerializerPackageTests.class);

	public static ColumnFamily<String, Long> CF_SERIALIZER1 = ColumnFamily
			.newColumnFamily(
					"Serializer1", 
					StringSerializer.get(),
					LongSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();

		keyspace.prepareQuery(CF_SERIALIZER1)
		.withCql("CREATE TABLE astyanaxunittests.serializer1 (key text, column1 bigint, value text, PRIMARY KEY (key))")
		.execute();

		CF_SERIALIZER1.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_SERIALIZER1);
	}

	@Test
	public void testSerializer() throws Exception {

		keyspace.prepareQuery(CF_SERIALIZER1)
		.withCql("select * from astyanaxunittests.serializer1")
		.execute();

		SerializerPackage serializer = keyspace.getSerializerPackage("Serializer1", false);

		System.out.println("");
		System.out.println("KeySerializer: " + serializer.getKeySerializer());
		System.out.println("ColumnNameSerializer: " + serializer.getColumnNameSerializer());
		System.out.println("ColumnSerializer: " + serializer.getColumnSerializer());
		System.out.println("DefaultValueSerializer: " + serializer.getDefaultValueSerializer());
		System.out.println("ValueSerializer: " + serializer.getValueSerializer());

		String ss1 = "ss1"; 
		ByteBuffer bb1 = StringSerializer.get().fromString(ss1);
		String ss1Result = serializer.getKeySerializer().getString(bb1);

		System.out.println("ss1Result: " + ss1Result);
		Assert.assertEquals(ss1, ss1Result);

		SpecificCompositeSerializer comp = (SpecificCompositeSerializer) serializer.getColumnNameSerializer();
		System.out.println(comp.getComparators().toString());

		Composite dc = new Composite(ss1);

		List<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
		types.add(UTF8Type.instance);

		CompositeType c1 = CompositeType.getInstance(types);

		SpecificCompositeSerializer ccSerializer = new SpecificCompositeSerializer(c1);
		ByteBuffer bb2 = ccSerializer.toByteBuffer(dc);

		Composite c2 = (Composite) serializer.getColumnNameSerializer().fromByteBuffer(bb2);
		ss1Result =  (String) c2.get(0);

		Assert.assertEquals(ss1, ss1Result);
	}
}
