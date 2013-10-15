package com.netflix.astyanax.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.db.marshal.UTF8Type;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.reads.CqlRangeBuilder;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

@SuppressWarnings("unused")
public class TestDriver {


	
	public static void main(String[] args) {

		
		CqlClusterImpl cluster = null;
		try {
//			
//			AstyanaxContext<com.netflix.astyanax.Cluster> context = new AstyanaxContext.Builder()
//				.forCluster("Test Cluster")
//				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
//				
//				.buildCluster(CqlFamilyFactory.getInstance());
//			
//			context.start();
//			
//			cluster = (CqlClusterImpl) context.getClient();

			
			Cluster cluster2 = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
//			Cluster cluster2 = Cluster.builder().addContactPoint("ec2-54-227-36-120.compute-1.amazonaws.com").withPort(7104).build();
			
//			String query = "begin batch insert into astyanaxunittests.test1 (key, column1) values (?, ?); insert into astyanaxunittests.test1 (key, column1) values (1, 8); insert into astyanaxunittests.test1 (key, column1) values (?, ?);  apply batch"; 
//			
//			//cluster2.connect().execute(query);
//			
//			PreparedStatement statement = cluster2.connect().prepare(query);
//			BoundStatement boundStatement = new BoundStatement(statement);
//			
//			boundStatement.bind(1,7, 1, 10);
			//boundStatement.bind(columnValue, rowKey);

//			cluster2.connect().execute(boundStatement);
//			
			
//			String query = "select * from astyanaxperf.test1 where key=300000;";
	
			String query = "begin unlogged batch insert into astyanaxunittests.test12 (key, column1) values (1, 8); apply batch"; 

			//ResultSet rs = cluster2.connect().execute(query);
			
			ExecutorService threadPool = Executors.newFixedThreadPool(1);

			final ResultSetFuture rsFuture = cluster2.connect().executeAsync(query);
			
			rsFuture.addListener(new Runnable() {

				@Override
				public void run() {
					
					try {
						ResultSet rs = rsFuture.get();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				
			}, threadPool);
			
			
			//System.out.println(rs.all().size());
			
			//cluster2.shutdown();
			
			
			//PreparedStatement pstmt = new PreparedSta/
			
			//executeSampleBoundStatement22();
			
			//cluster = new CqlClusterImpl();
			
			//createKeyspace(cluster);
			//createTable(cluster);
			//truncateTable(cluster);
			//insertIntoTable(cluster);

//			MetricsRegistry registry = cluster.cluster.getMetrics().getRegistry();
//			ConsoleReporter.enable(registry, 5, TimeUnit.SECONDS);

//			// reads
//			while (true) {
//				Thread.sleep(2000);
//				readMultipleColumnsFromTable(cluster);
//			}
			//readRowCount(cluster);
			//describeClusterAndKeyspace(cluster);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}
	
	private static void describeClusterAndKeyspace(CqlClusterImpl cluster) throws Exception {
		
	        System.out.println("Describe cluster: " + cluster.describeClusterName());
	        
	        KeyspaceDefinition ksDef = cluster.describeKeyspace("puneet");
	        System.out.println("Name: " + ksDef.getName());
	        System.out.println("getStrategyClass: " + ksDef.getStrategyClass());
	        System.out.println("getStrategyOptions: " + ksDef.getStrategyOptions().toString());
	        
	        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily("person");
	        System.out.println("CfDef Name " + cfDef.getName());
	        System.out.println("CfDef  getProperties " + cfDef.getProperties().toString());
	        List<ColumnDefinition> cfDefs = cfDef.getColumnDefinitionList();
	        
	        for (ColumnDefinition colDef : cfDefs) {
	            System.out.println("Cfdef : " + colDef.getName());
	            System.out.println("Cfdef getOptions : " + colDef.getOptions());
	        }
	}
	
	private static void executeCqlDirectlyPreparedStatment(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<CqlResult<String, String>> result = ks.prepareQuery(cf)
				.withCql("SELECT * from puneet.scores where name = ? and score <= ?")
				.asPreparedStatement()
				.withStringValue("bob")
				.withIntegerValue(40)
				.execute();
		
		CqlResult<String, String> rows = result.getResult();
		
		printRows(rows.getRows());
		
		rows = ks.prepareQuery(cf)
				.withCql("SELECT count(*) from puneet.scores where name = ?")
				.asPreparedStatement()
				.withStringValue("joe")
				.execute().getResult();
		
		System.out.println("Is number: " + rows.hasNumber() + ", has rows: " + rows.hasRows() + ", number: " + rows.getNumber());
	}
	
	private static void executeCqlDirectly(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		
		OperationResult<CqlResult<String, String>> result = ks.prepareQuery(cf)
				.withCql("SELECT * from puneet.scores where name ='bob'")
				.execute();
		
		
		CqlResult<String, String> rows = result.getResult();
		
		printRows(rows.getRows());
		
		rows = ks.prepareQuery(cf)
				.withCql("SELECT count(*) from puneet.scores where name ='joe'")
				.execute().getResult();
		System.out.println("Is number: " + rows.hasNumber() + ", has rows: " + rows.hasRows() + ", number: " + rows.getNumber());
	}

	private static void readMultipleRowsAndColumnRange(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<Rows<String, String>> result = ks.prepareQuery(cf)
				.getRowSlice("bob", "joe")
				.withColumnRange(new CqlRangeBuilder<Integer>()
						.setColumn("score")
						.setStart(41)
						.setEnd(52)
						.build()).execute();
		
		Rows<String, String> rows = result.getResult();
		for (com.netflix.astyanax.model.Row<String, String> row : rows) {
			
			String rowKey = row.getKey();
			System.out.print(" " + rowKey + " ==>    ");
			ColumnList<String> colList = row.getColumns();
			Column<String> col = colList.getColumnByIndex(0); 
			System.out.print(" " + col.getName() + " = " + col.getIntegerValue());
			col = colList.getColumnByIndex(1); 
			System.out.print(",  " + col.getName() + " = " + col.getDateValue());
			System.out.println();
		}
	}
	
	private static void readMultipleRowsAndColumnCount(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<Map<String, Integer>> result = ks.prepareQuery(cf)
				.getRowSlice("bob", "joe")
				.withColumnSlice("score", "date")
				.getColumnCounts().execute();
		
		Map<String, Integer> map = result.getResult();
		for (String key : map.keySet()) {
			System.out.println("Num cols: " + key + " = " + map.get(key));
		}
	}
	
	private static void readMultipleRowsMultiKeysWithColumnSpec(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<Rows<String, String>> result = ks.prepareQuery(cf)
				.getRowSlice("bob", "joe")
				.withColumnSlice("score", "date").execute();
		
		System.out.println("Num rows: " + result.getResult().size());
		printRows(result.getResult());
	}
	
	private static void printRows(Rows<String, String> rows) {
		for (com.netflix.astyanax.model.Row<String, String> row : rows) {
			
			String rowKey = row.getKey();
			System.out.print(" " + rowKey + " ==>    ");
			ColumnList<String> colList = row.getColumns();
			Column<String> col = colList.getColumnByIndex(0); 
			System.out.print(" " + col.getName() + " = " + col.getIntegerValue());
			col = colList.getColumnByIndex(1); 
			System.out.print(",   " + col.getName() + " = " + col.getDateValue());
			
			System.out.println();
		}
	}
	
	private static void readMultipleRowsMultiKeys(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<Rows<String, String>> result = ks.prepareQuery(cf).getRowSlice("bob", "joe").execute();
		System.out.println("Num rows: " + result.getResult().size());
		
		result = ks.prepareQuery(cf).getRowSlice("bob").execute();
		System.out.println("Num rows: " + result.getResult().size());

		result = ks.prepareQuery(cf).getRowSlice("joe").execute();
		System.out.println("Num rows: " + result.getResult().size());
	}	
	
	private static void readSingleRowWithColumnRangeStartEndColumnAndLimit(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<ColumnList<String>> result = ks.prepareQuery(cf)
														.getRow("bob")
														.withColumnRange(new CqlRangeBuilder<Integer>()
																.setColumn("score")
																.setStart(35)
																.setEnd(46)
																.setLimit(1).build())
														.execute();
		
		ColumnList<String> colList = result.getResult();
		
		Iterator<Column<String>> iter = colList.iterator();
		
		while (iter.hasNext()) {
			Column<String> column = iter.next();
			System.out.println("Col: " + column.getName() + " " + column.getDateValue());
		}
	}	
	
	private static void readSingleRowWithColumnRangeStartEndColumn(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<ColumnList<String>> result = ks.prepareQuery(cf)
														.getRow("bob")
														.withColumnRange(new CqlRangeBuilder<Integer>()
																.setColumn("score")
																.setStart(35)
																.setEnd(46).build())
														.execute();
		
		ColumnList<String> colList = result.getResult();
		
		Iterator<Column<String>> iter = colList.iterator();
		
		while (iter.hasNext()) {
			Column<String> column = iter.next();
			System.out.println("Col: " + column.getName() + " " + column.getDateValue());
		}
	}
	
	private static void readSingleRowWithColumnRangeEndColumn(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<ColumnList<String>> result = ks.prepareQuery(cf)
														.getRow("bob")
														.withColumnRange(new CqlRangeBuilder<Integer>().setColumn("score").setEnd(35).build())
														.execute();
		
		ColumnList<String> colList = result.getResult();
		
		Iterator<Column<String>> iter = colList.iterator();
		
		while (iter.hasNext()) {
			Column<String> column = iter.next();
			System.out.println("Col: " + column.getName() + " " + column.getDateValue());
		}
	}
	
	private static void readSingleRowWithColumnRangeStartColumn(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<ColumnList<String>> result = ks.prepareQuery(cf)
														.getRow("bob")
														.withColumnRange(new CqlRangeBuilder<Integer>().setColumn("score").setStart(35).build())
														.execute();
		
		ColumnList<String> colList = result.getResult();
		
		Iterator<Column<String>> iter = colList.iterator();
		
		while (iter.hasNext()) {
			Column<String> column = iter.next();
			System.out.println("Col: " + column.getName() + " " + column.getDateValue());
		}
	}

	private static void readSingleRowWithColumnRangeLimit1(CqlClusterImpl cluster) throws Exception {

		Session session = cluster.cluster.connect();
		
		Keyspace ks = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		cf.setKeyAlias("name");
		
		OperationResult<ColumnList<String>> result = ks.prepareQuery(cf)
														.getRow("bob")
														.withColumnRange(new CqlRangeBuilder<Integer>().setLimit(1).build())
														.execute();
		
		ColumnList<String> colList = result.getResult();
		
		System.out.println("Row col names: " + colList.getColumnNames().toString());
		System.out.println("Row col0: " + colList.getColumnByIndex(0).getName() + " = " + colList.getColumnByIndex(0).getStringValue());
		System.out.println("Row col1: " + colList.getColumnByIndex(1).getName() + " = " + colList.getColumnByIndex(1).getIntegerValue());
		System.out.println("Row col2: " + colList.getColumnByIndex(2).getName() + " = " + colList.getColumnByIndex(2).getDateValue());
	}
	
	private static void readRowCount(CqlClusterImpl cluster) throws Exception {
		
		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("scores", StringSerializer.get(), StringSerializer.get());
		
		cf.setKeyAlias("name");
		
		Keyspace ks = cluster.getKeyspace("puneet");
		
		OperationResult<Integer> result = ks.prepareQuery(cf)
														.getRow("bob")
														.getCount()
														.execute();
		
		System.out.println("Row count: " + result.getResult());
	}

	private static void readMultipleColumnsFromTable(CqlClusterImpl cluster) throws Exception {
		
		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());
		Keyspace ks = cluster.getKeyspace("puneet");
		
		Collection<String> colNames = new ArrayList<String>();
		colNames.add("average_size"); 
		colNames.add("common_name"); 
		colNames.add("population"); 
		
		final ListenableFuture<OperationResult<ColumnList<String>>> asyncResult = ks.prepareQuery(cf)
		.getRow("baboon1")
		.withColumnSlice(colNames)
		.executeAsync();

		
		asyncResult.addListener(new Runnable() {

			@Override
			public void run() {
				System.out.println("Inside async");
				ColumnList<String> colList;
				try {
					colList = asyncResult.get().getResult();
					System.out.println("Col list size: " + colList.size());
					System.out.println("average_size: " + colList.getColumnByName("average_size").getIntegerValue());
					System.out.println("common_name: " + colList.getColumnByName("common_name").getStringValue());
					System.out.println("population: " + colList.getColumnByName("population").getIntegerValue());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}, Executors.newFixedThreadPool(1));
	}
	
	private static void readSingleColumnFromTable(CqlClusterImpl cluster) throws Exception {
		
		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());
		Keyspace ks = cluster.getKeyspace("puneet");
		
		OperationResult<Column<String>> result = ks.prepareQuery(cf)
														.getRow("baboon1")
														.getColumn("average_size").execute();
		
		System.out.println("Avg size: " + result.getResult().getIntegerValue());
	}

	private static void createKeyspace(com.netflix.astyanax.Cluster cluster) throws Exception {
		
//		Properties props = new Properties();
		
//		props.setProperty("strategy_options.replication_factor", "1");
//		props.setProperty("strategy_class", "SimpleStrategy");
//		props.setProperty("durable_writes", "true");

//		props.setProperty("replication.class", "SimpleStrategy");
//		props.setProperty("replication.replication_factor", "2");
//		props.setProperty("durable_writes", "true");

//		CqlKeyspaceDefinitionImpl ksDef = (CqlKeyspaceDefinitionImpl) cluster.makeKeyspaceDefinition();
//		
//		ksDef.setName("test1")
//			 .setStrategyClass("SimpleStrategy")
//			 .setStrategyOptions(ImmutableMap.<String, String>builder()
//				        .put("replication_factor", "1")
//				        .build());
//		
//		ksDef.execute();
		
		Keyspace keyspace = cluster.getKeyspace("test2"); 
		
		// Using simple strategy
//		keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
//		    .put("strategy_options", ImmutableMap.<String, Object>builder()
//		        .put("replication_factor", "4")
//		        .build())
//		    .put("strategy_class",     "SimpleStrategy")
//		        .build()
//		     );

		// Using network topology
		keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
		    .put("strategy_options", ImmutableMap.<String, Object>builder()
		        .put("us-east", "3")
		        .put("eu-west", "3")
		        .build())
		    .put("strategy_class",     "NetworkTopologyStrategy")
		    .build()
		     );
	}
	
	private static void createTable(com.netflix.astyanax.Cluster cluster) throws Exception {
		
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cluster.makeColumnFamilyDefinition();
		cfDef.setName("person")
			 .setKeyspace("puneet")
			 .setKeyValidationClass("UTF8Type")
			 .setKeyAlias(UTF8Type.instance.decompose("key"));
		
		cfDef.makeColumnDefinition()
			 .setName("nickname")
			 .setValidationClass("UTF8Type");
	
		cfDef.makeColumnDefinition()
		 .setName("age")
		 .setValidationClass("Int32Type");
		
		cfDef.setBloomFilterFpChance(0.01d)
			 .setComment("my own table")
			 .setReadRepairChance(1d)
			 .setReplicateOnWrite(true);
		
		cfDef.execute();
	}
	
	private static void dropTable(Cluster cluster) {
		
		ResultSet result = cluster.connect().execute("DROP TABLE puneet.monkeyspecies");
	}
	
	public static void execDirectQuery(Cluster cluster, String query) {
		
		Session session = cluster.connect();
		ResultSet result = session.execute(query);
		
		System.out.println("ResultSet : " + result.toString());
	}
	
	
	private static void executeSampleBoundStatement22() {

//		Binding  = 1.0
//		Binding bloom_filter_fp_chance = 0.01
//		Binding comment = my own table
//		Binding replicate_on_write = true

		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		
		Session session = cluster.connect();
//		PreparedStatement statement = session.prepare(
//				"CREATE TABLE puneet.person ( key text PRIMARY KEY, nickname text, age int) WITH ? = ? ");
//		//AND ? = ? AND ? = ? AND ? = ?");
//		
//		BoundStatement boundStatement = new BoundStatement(statement);
//		session.execute(boundStatement.bind("read_repair_chance", 1.0f));
		
		session.execute("CREATE TABLE puneet.person ( key text PRIMARY KEY, nickname text, age int) WITH read_repair_chance = 0.3");
	}

					
	private static void executeSampleBoundStatement() {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		
		Session session = cluster.connect();
		PreparedStatement statement = session.prepare(
			      "INSERT INTO puneet.monkeyspecies " +
			      "(key, average_size, common_name, population) " +
			      "VALUES (?, ?, ?, ?);");
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("b3", 1, "onee", 11));
	}

	private static void executeSampleBoundStatement2() {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		
		Session session = cluster.connect();
		PreparedStatement statement = session.prepare(
			      "DELETE FROM puneet.monkeyspecies WHERE key = ?");
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("b3"));
	}

	private static void executeSampleBoundStatement3() {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		
		Session session = cluster.connect();
		PreparedStatement statement = session.prepare(
			      "UPDATE puneet.monkeyspecies SET average_size = ? , common_name = ? WHERE key = ?");
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(11, "new_common_name", "b2"));
	}


	private static void truncateTable(com.netflix.astyanax.Cluster cluster) throws Exception {

		Keyspace keyspace = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());

		keyspace.truncateColumnFamily(cf);
	}

	private static void insertIntoTable(com.netflix.astyanax.Cluster cluster) throws Exception {

		Keyspace keyspace = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());

		MutationBatch batch = keyspace.prepareMutationBatch();
		
		for (int i=1; i<=10; i++) {
			batch.withRow(cf, "baboon" + i)
			.putColumn("average_size", i)
			.putColumn("common_name", "cm_name" + i)
			.putColumn("population", i*10);
		}

		batch.execute();
	}
	
	private static void deleteRowFromTable(com.netflix.astyanax.Cluster cluster) throws Exception {

		Keyspace keyspace = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());

		MutationBatch batch = keyspace.prepareMutationBatch();
		batch.withRow(cf, "baboon11")
		.delete();

		batch.execute();
	}
	
	
	private static void updateColumnInTable(com.netflix.astyanax.Cluster cluster) throws Exception {

		Keyspace keyspace = cluster.getKeyspace("puneet");

		ColumnFamily<String, String> cf = 
				new ColumnFamily<String, String>("monkeyspecies", StringSerializer.get(), StringSerializer.get());

		MutationBatch batch = keyspace.prepareMutationBatch();
		batch.withRow(cf, "baboon")
		.putColumn("average_size", 12)
		.putColumn("common_name", "new12_common_name")
		.deleteColumn("population");

		batch.execute();
	}

	private static void describeCluster(com.netflix.astyanax.Cluster cluster) throws Exception {

		System.out.println("Cluster Name: " + cluster.describeClusterName());
		System.out.println("Cluster Version: " + cluster.getVersion());
		System.out.println("Cluster Version: " + cluster.describePartitioner());
	}
	
	
}