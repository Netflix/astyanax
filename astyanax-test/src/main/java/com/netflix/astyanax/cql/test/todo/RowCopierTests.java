package com.netflix.astyanax.cql.test.todo;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.utils.AstyanaxContextFactory;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration.Driver;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class RowCopierTests extends KeyspaceTests {

	private static final Driver driver = Driver.JAVA_DRIVER;
	//private static final Driver driver = Driver.THRIFT;
	
	@BeforeClass
	public static void init() throws Exception {
		ClusterConfiguration.setDriver(driver);
		initContext();
	}
	
	@Test
	public void runRowCopyTest() throws Exception {

		String keyspaceName = "AstyanaxUnitTests".toLowerCase();
		
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
    	context.start();
        keyspace = context.getClient();

		ColumnFamily<Integer, String> cf = 
				new ColumnFamily<Integer, String>("testrowcopy", IntegerSerializer.get(), StringSerializer.get(), IntegerSerializer.get());
		ColumnFamily<Integer, String> cf2 = 
				new ColumnFamily<Integer, String>("testrowcopy2", IntegerSerializer.get(), StringSerializer.get(), IntegerSerializer.get());

		keyspace.createColumnFamily(cf, null);
		keyspace.createColumnFamily(cf2, null);
		
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(cf, 10).putColumn("c1", 1).putColumn("c2", 2);
		m.execute();
		
		ColumnList<String> result = keyspace.prepareQuery(cf).getRow(10).execute().getResult();
		
		Column<String> column = result.getColumnByIndex(0);
		Assert.assertEquals("c1", column.getName());
		Assert.assertEquals(1, column.getIntegerValue());
		column = result.getColumnByIndex(1);
		Assert.assertEquals("c2", column.getName());
		Assert.assertEquals(2, column.getIntegerValue());
		
		keyspace.prepareQuery(cf).getRow(10).copyTo(cf2, 11).execute();
		
		ColumnList<String> result2 = keyspace.prepareQuery(cf2).getRow(11).execute().getResult();
		
		column = result2.getColumnByIndex(0);
		Assert.assertEquals("c1", column.getName());
		Assert.assertEquals(1, column.getIntegerValue());
		column = result2.getColumnByIndex(1);
		Assert.assertEquals("c2", column.getName());
		Assert.assertEquals(2, column.getIntegerValue());
		
		keyspace.dropColumnFamily(cf);
		keyspace.dropColumnFamily(cf2);
	}
}
