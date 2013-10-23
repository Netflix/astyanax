package com.netflix.astyanax.cql.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class FooTest extends KeyspaceTests {

		private static final Driver driver = Driver.JAVA_DRIVER;
		//private static final Driver driver = Driver.THRIFT;
		
		@BeforeClass
		public static void init() throws Exception {
			ClusterConfiguration.setDriver(driver);
			initContext();
		}
		
		public static class Population {
			
			@Component(ordinal=0) String state;
			@Component(ordinal=1) String city;
			@Component(ordinal=2) Integer zip;
			
			  // Must have public default constructor
			  public Population() {
			  }
			  public Population(String s, String c, int z) {
				  state = s; city = c; zip = z;
			  }
			  public String toString() {
				  return state + ":" + city + ":" + zip;
			  }
		}
	
		//@Test
		public void testFoo1() throws Exception {
			
			Keyspace ks = super.keyspace;
			
			ColumnFamily<String, String> cf = new ColumnFamily<String, String>("accounts", StringSerializer.get(), StringSerializer.get());
			 ColumnFamilyDefinition cfDef = cf.describe(ks);
			 
			 List<ColumnDefinition> colDefs = cfDef.getColumnDefinitionList();
			 
			 for (ColumnDefinition col : colDefs) {
				 System.out.println("Col " + col.getName() + " " + col.getValidationClass());
			 }

			 MutationBatch batch = ks.prepareMutationBatch();
			 batch.withRow(cf, "a").deleteColumn("user");
			 batch.withRow(cf, "aa").deleteColumn("pswd");
			 batch.execute();
			 
			 ColumnList<String> result1 = ks.prepareQuery(cf).getRow("aa").execute().getResult();
			 
			 Column<String> c = result1.getColumnByName("pswd");
			 System.out.println(c.getName() + " " + c.getStringValue());
			 Column<String> c1 = result1.getColumnByName("user");
			 System.out.println(c1.getName() + " " + c1.getStringValue());
			 
			 for (Column<String> col : result1) {
				 System.out.println(col.getName() + " " + col.getStringValue());
			 }

			 //Rows<String, String> result = ks.prepareQuery(cf).getRowSlice("a", "aa", "b").execute().getResult();

			 Rows<String, String> result = ks.prepareQuery(cf).getRowSlice("a", "aa", "b")
					 .withColumnSlice("user", "pswd").execute().getResult();

			 for (Row<String, String> row : result) {
				 
				 System.out.println("\nRow: " + row.getKey());
				 ColumnList<String> cols = row.getColumns();
				 for (Column<String> col : cols) {
					 System.out.println("Col: " + col.getName() + " " + col.getStringValue());
				 }
			 }
			 
			 System.out.println("SINGLE COLUMN QUERY");

			 Column<String> result3 = ks.prepareQuery(cf).getRow("a").getColumn("user").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
			 result3 = ks.prepareQuery(cf).getRow("a").getColumn("pswd").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());

			 result3 = ks.prepareQuery(cf).getRow("aa").getColumn("user").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
			 result3 = ks.prepareQuery(cf).getRow("aa").getColumn("pswd").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());

			 result3 = ks.prepareQuery(cf).getRow("b").getColumn("user").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
			 result3 = ks.prepareQuery(cf).getRow("b").getColumn("pswd").execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
		}


		//@Test
		public void testFoo2() throws Exception {
			
			Keyspace ks = super.keyspace;
			
			ColumnFamily<Integer, Integer> cf = new ColumnFamily<Integer, Integer>("test1", IntegerSerializer.get(), IntegerSerializer.get(), StringSerializer.get());
			 ColumnFamilyDefinition cfDef = cf.describe(ks);
			 
			 List<ColumnDefinition> colDefs = cfDef.getColumnDefinitionList();
			 
			 for (ColumnDefinition col : colDefs) {
				 System.out.println("Col " + col.getName() + " " + col.getValidationClass());
			 }

			 MutationBatch batch = ks.prepareMutationBatch();
			 batch.withRow(cf, 2).putColumn(1, "ff");
			 batch.withRow(cf, 2).putColumn(2, "fff");
			 batch.withRow(cf, 1).deleteColumn(7).putColumn(8, "get");
			 batch.withRow(cf, 1).deleteColumn(7).putColumn(10, "ten");
			 batch.execute();
			 
			 System.out.println("FULL ROW QUERY");
			 ColumnList<Integer> result = ks.prepareQuery(cf).getRow(1).execute().getResult();
			 
			 Column<Integer> c = result.getColumnByName(8);
			 System.out.println(c.getName() + " " + c.getStringValue());
			 Column<Integer> c1 = result.getColumnByName(11);
			 System.out.println(c1.getName() + " " + c1.getStringValue());
			 
			 System.out.println();
			 for (Column<Integer> col : result) {
				 System.out.println(col.getName() + " " + col.getStringValue());
			 }
			 
			 System.out.println("COL SLICE QUERY");
			 result = ks.prepareQuery(cf).getRow(1).withColumnSlice(8,11).execute().getResult();
			 
			 c = result.getColumnByName(8);
			 System.out.println(c.getName() + " " + c.getStringValue());
			 c1 = result.getColumnByName(11);
			 System.out.println(c1.getName() + " " + c1.getStringValue());
			 
			 System.out.println();
			 for (Column<Integer> col : result) {
				 System.out.println(col.getName() + " " + col.getStringValue());
			 }

			 System.out.println("COL RANGE QUERY");
			 result = ks.prepareQuery(cf).getRow(1).withColumnRange(8, 11, true, 10).execute().getResult();
			 
			 
			 System.out.println();
			 for (Column<Integer> col : result) {
				 System.out.println(col.getName() + " " + col.getStringValue());
			 }
			 
			 //Rows<Integer, Integer> result = ks.prepareQuery(cf).getRowSlice(1, 2, 3).execute().getResult();

//			 Rows<Integer, Integer> result = ks.prepareQuery(cf).getRowSlice(1, 2, 3)
//			 .withColumnSlice(1,2,3,8,11).execute().getResult();

			 Rows<Integer, Integer> result1 = ks.prepareQuery(cf).getRowSlice(1, 2, 3)
			 .withColumnRange(1, 5, false, 10)
			 .execute().getResult();

			 for (Row<Integer, Integer> row : result1) {
				 
				 System.out.println("\nRow: " + row.getKey());
				 ColumnList<Integer> cols = row.getColumns();
				 for (Column<Integer> col : cols) {
					 System.out.println("Col: " + col.getName() + " " + col.getStringValue());
				 }
			 }

			 System.out.println("SINGLE COLUMN QUERY");

			 Column<Integer> result3 = ks.prepareQuery(cf).getRow(1).getColumn(8).execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
			 result3 = ks.prepareQuery(cf).getRow(1).getColumn(10).execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getStringValue());
			 
			 System.out.println("SINGLE COLUMN COUNT QUERY");

			 int result4 = ks.prepareQuery(cf).getRow(1).getCount().execute().getResult();
			 System.out.println("Count: " + result4);
			 result4 = ks.prepareQuery(cf).getRow(1).withColumnRange(9, 11, false, 10).getCount().execute().getResult();
			 System.out.println("Count: " + result4);

			 System.out.println("ROW RANGE COLUMN COUNT QUERY");

			 Map<Integer, Integer> result5 = ks.prepareQuery(cf).getRowSlice(1,2,3).getColumnCounts().execute().getResult();
			 for (Integer key : result5.keySet()) {
				 System.out.println("Key: " + key + " Count: " + result5.get(key));
			 }

			 System.out.println();
			 result5 = ks.prepareQuery(cf).getRowSlice(1,2,3).withColumnSlice(1,2,8).getColumnCounts().execute().getResult();
			 for (Integer key : result5.keySet()) {
				 System.out.println("Key: " + key + " Count: " + result5.get(key));
			 }

			 System.out.println();
			 result5 = ks.prepareQuery(cf).getRowSlice(1,2,3).withColumnRange(6, 11, false, 10).getColumnCounts().execute().getResult();
			 for (Integer key : result5.keySet()) {
				 System.out.println("Key: " + key + " Count: " + result5.get(key));
			 }
		}

		@Test
		public void testFoo3() throws Exception {
			
			Keyspace ks = super.keyspace;
			
			AnnotatedCompositeSerializer<Population> compSerializer = new AnnotatedCompositeSerializer<Population>(Population.class);
			ColumnFamily<Integer, Population> cf = 
					new ColumnFamily<Integer, Population>("population", IntegerSerializer.get(), compSerializer, IntegerSerializer.get());
			ColumnFamilyDefinition cfDef = cf.describe(ks);
			 
			List<ColumnDefinition> colDefs = cfDef.getColumnDefinitionList();
			 
			 for (ColumnDefinition col : colDefs) {
				 System.out.println("Col " + col.getName() + " " + col.getValidationClass());
			 }

			 MutationBatch batch = ks.prepareMutationBatch();
			 
			 Population p1 = new Population("CA", "SF", 1);
			 Population p2 = new Population("CA", "SF", 2);
			 Population p3 = new Population("CA", "SD", 1);
			 
			 batch.withRow(cf, 2013).putColumn(p1, 1000);
			 batch.withRow(cf, 2013).putColumn(p2, 200);
			 batch.withRow(cf, 2013).putColumn(p3, 3000);
			 
			 batch.execute();
			 
			 System.out.println("FULL ROW QUERY");
			 ColumnList<Population> result1 = ks.prepareQuery(cf).getRow(2012).execute().getResult();
			 
			 System.out.println();
			 for (Column<Population> col : result1) {
				 System.out.println(col.getName() + " " + col.getIntegerValue());
			 }

			 System.out.println("\nCOL RANGE ROW QUERY");
			 result1 = ks.prepareQuery(cf).getRow(2012).withColumnRange(compSerializer.buildRange()
					 													.withPrefix("CA")
					 													.greaterThanEquals("SD")
					 													.build()).execute().getResult();
			 
			 System.out.println();
			 for (Column<Population> col : result1) {
				 System.out.println(col.getName() + " " + col.getIntegerValue());
			 }
			
			 //Rows<Integer, Population> result = ks.prepareQuery(cf).getRowSlice(2012).execute().getResult();
			 
			 Rows<Integer, Population> result = ks.prepareQuery(cf).getRowSlice(2012, 2013)
					 		.withColumnRange(compSerializer.buildRange()
					 								.withPrefix("CA")
					 								.withPrefix("SF")
					 								.lessThanEquals(2))
					 .execute().getResult();

			 for (Row<Integer, Population> row : result) {
				 
				 System.out.println("\nRow: " + row.getKey());
				 ColumnList<Population> cols = row.getColumns();
				 for (Column<Population> col : cols) {
					 System.out.println("Col: " + col.getName() + " " + col.getIntegerValue());
				 }
			 }

			 System.out.println("SINGLE COLUMN QUERY");

			 Column<Population> result3 = ks.prepareQuery(cf).getRow(2012).getColumn(p1).execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getIntegerValue());

			 result3 = ks.prepareQuery(cf).getRow(2013).getColumn(p3).execute().getResult();
			 System.out.println("Column: " + result3.getName() + " " + result3.getIntegerValue());
			 
			 
			 System.out.println("SINGLE ROW COLUMN COUNT QUERY");

			 int result4 = ks.prepareQuery(cf).getRow(2012).withColumnRange(compSerializer.buildRange()
						.withPrefix("CA")
						.withPrefix("SF")
						.greaterThanEquals(2)
						.build())
						.getCount()
						.execute().getResult();
			 
			 System.out.println("Count: " + result4);
			 
			 System.out.println("ROW RANGE COLUMN COUNT QUERY");

			 Map<Integer, Integer> result5 = ks.prepareQuery(cf).getRowSlice(2012,2013).getColumnCounts().execute().getResult();
			 for (Integer key : result5.keySet()) {
				 System.out.println("Key: " + key + " Count: " + result5.get(key));
			 }
			 
			 System.out.println();
			 result5 = ks.prepareQuery(cf).getRowSlice(2012,2013)
					 .withColumnRange(compSerializer.buildRange()
								.withPrefix("CA")
								.greaterThanEquals("SD")
								.build())
								.getColumnCounts().execute().getResult();
			 for (Integer key : result5.keySet()) {
				 System.out.println("Key: " + key + " Count: " + result5.get(key));
			 }

		}	
	
    private static AstyanaxContext<Keyspace> initWithJavaDriver(String clusterName) {

    	
    	final List<Host> hosts = new ArrayList<Host>();
    	
    	hosts.add(new Host("ec2-54-224-6-184.compute-1.amazonaws.com", 7104));
    	hosts.add(new Host("ec2-54-225-41-34.compute-1.amazonaws.com", 7104));
    	hosts.add(new Host("ec2-204-236-195-101.compute-1.amazonaws.com", 7104));
    	hosts.add(new Host("ec2-50-17-113-110.compute-1.amazonaws.com", 7104));
    	hosts.add(new Host("ec2-50-17-97-71.compute-1.amazonaws.com", 7104));
    	hosts.add(new Host("ec2-54-221-122-246.compute-1.amazonaws.com", 7104));
        

		Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			@Override
			public List<Host> get() {
				return hosts;
			}
    	};
    	
    	AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster(clusterName)
                .forKeyspace("astyanaxperf")
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.JAVA_DRIVER)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("cass_poberai_perf1"
                                + "_" + "astyanaxperf")
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                .setPort(7104)
                                )
                .withHostSupplier(HostSupplier)
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(CqlFamilyFactory.getInstance());

    	return context;
    }
}
