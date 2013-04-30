package com.netflix.astyanax.examples;

import static com.netflix.astyanax.examples.ModelConstants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Example code for demonstrating how to access Cassandra using Astyanax and CQL3.
 * 
 * @author elandau
 * @author Marko Asplund
 */
public class AstCQLClient {
  private static final Logger logger = LoggerFactory.getLogger(AstCQLClient.class);
  
  private AstyanaxContext<Keyspace> context;
  private Keyspace keyspace;
  private ColumnFamily<Integer, String> EMP_CF;
  private static final String EMP_CF_NAME = "employees1";
  private static final String INSERT_STATEMENT =
      String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?);",
          EMP_CF_NAME, COL_NAME_EMPID, COL_NAME_DEPTID, COL_NAME_FIRST_NAME, COL_NAME_LAST_NAME);
  private static final String CREATE_STATEMENT =
      String.format("CREATE TABLE %s (%s int, %s int, %s varchar, %s varchar, PRIMARY KEY (%s, %s))",
          EMP_CF_NAME, COL_NAME_EMPID, COL_NAME_DEPTID, COL_NAME_FIRST_NAME, COL_NAME_LAST_NAME,
          COL_NAME_EMPID, COL_NAME_DEPTID);

  public void init() {
    logger.debug("init()");
    
    context = new AstyanaxContext.Builder()
    .forCluster("Test Cluster")
    .forKeyspace("test1")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(9160)
        .setMaxConnsPerHost(1)
        .setSeeds("127.0.0.1:9160")
    )
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setCqlVersion("3.0.0")
        .setTargetCassandraVersion("1.2"))
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    keyspace = context.getEntity();
    
    EMP_CF = ColumnFamily.newColumnFamily(
        EMP_CF_NAME, 
        IntegerSerializer.get(), 
        StringSerializer.get());
  }
  
  public void insert(int empId, int deptId, String firstName, String lastName) {
    try {
      @SuppressWarnings("unused")
      OperationResult<CqlResult<Integer, String>> result = keyspace
          .prepareQuery(EMP_CF)
              .withCql(INSERT_STATEMENT)
          .asPreparedStatement()
              .withIntegerValue(empId)
              .withIntegerValue(deptId)
              .withStringValue(firstName)
              .withStringValue(lastName)
          .execute();
    } catch (ConnectionException e) {
      logger.error("failed to write data to C*", e);
      throw new RuntimeException("failed to write data to C*", e);
    }
    logger.debug("insert ok");
  }
  
  public void insertDynamicProperties(int id, String[] ... entries) {
    MutationBatch m = keyspace.prepareMutationBatch();

    ColumnListMutation<String> clm = m.withRow(EMP_CF, id);
    for(String[] kv : entries) {
      clm.putColumn(kv[0], kv[1], null);
    }
    
    try {
      @SuppressWarnings("unused")
      OperationResult<Void> result = m.execute();
    } catch (ConnectionException e) {
      logger.error("failed to write data to C*", e);
      throw new RuntimeException("failed to write data to C*", e);
    }
    logger.debug("insert ok");
  }

  
  public void createCF() {
    logger.debug("CQL: "+CREATE_STATEMENT);
    try {
      @SuppressWarnings("unused")
      OperationResult<CqlResult<Integer, String>> result = keyspace
          .prepareQuery(EMP_CF)
          .withCql(CREATE_STATEMENT)
          .execute();
    } catch (ConnectionException e) {
      logger.error("failed to create CF", e);
      throw new RuntimeException("failed to create CF", e);
    }
  }

  public void read(int empId) {
    logger.debug("read()");
    try {
      OperationResult<CqlResult<Integer, String>> result
        = keyspace.prepareQuery(EMP_CF)
          .withCql(String.format("SELECT * FROM %s WHERE %s=%d;", EMP_CF_NAME, COL_NAME_EMPID, empId))
          .execute();
      for (Row<Integer, String> row : result.getResult().getRows()) {
        logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
        
        ColumnList<String> cols = row.getColumns();
        logger.debug("emp");
        logger.debug("- emp id: "+cols.getIntegerValue(COL_NAME_EMPID, null));
        logger.debug("- dept: "+cols.getIntegerValue(COL_NAME_DEPTID, null));
        logger.debug("- firstName: "+cols.getStringValue(COL_NAME_FIRST_NAME, null));
        logger.debug("- lastName: "+cols.getStringValue(COL_NAME_LAST_NAME, null));
      }
    } catch (ConnectionException e) {
      logger.error("failed to read from C*", e);
      throw new RuntimeException("failed to read from C*", e);
    }
  }
  
  public static void main(String[] args) {
    logger.debug("main");
    AstCQLClient c = new AstCQLClient();
    c.init();
//    c.createCF();
    c.insert(222, 333, "Eric", "Cartman");
    c.read(222);
  }

}
