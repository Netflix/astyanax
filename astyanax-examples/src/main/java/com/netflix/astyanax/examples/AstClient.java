package com.netflix.astyanax.examples;

import static com.netflix.astyanax.examples.ModelConstants.*;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


/**
 * Example code for demonstrating how to access Cassandra using Astyanax.
 * 
 * @author elandau
 * @author Marko Asplund
 */
public class AstClient {
  private static final Logger logger = LoggerFactory.getLogger(AstClient.class);

  private AstyanaxContext<Keyspace> context;
  private Keyspace keyspace;
  private ColumnFamily<Integer, String> EMP_CF;
  private static final String EMP_CF_NAME = "employees2";

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
    MutationBatch m = keyspace.prepareMutationBatch();

    m.withRow(EMP_CF, empId)
      .putColumn(COL_NAME_EMPID, empId, null)
      .putColumn(COL_NAME_DEPTID, deptId, null)
      .putColumn(COL_NAME_FIRST_NAME, firstName, null)
      .putColumn(COL_NAME_LAST_NAME, lastName, null)
      ;

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
  }
  
  public void read(int empId) {
    OperationResult<ColumnList<String>> result;
    try {
      result = keyspace.prepareQuery(EMP_CF)
        .getKey(empId)
        .execute();

      ColumnList<String> cols = result.getResult();
      logger.debug("read: isEmpty: "+cols.isEmpty());
      
      // process data

      // a) iterate over columsn 
      logger.debug("emp");
      for(Iterator<Column<String>> i = cols.iterator(); i.hasNext(); ) {
        Column<String> c = i.next();
        Object v = null;
        if(c.getName().endsWith("id")) // type induction hack
          v = c.getIntegerValue();
        else
          v = c.getStringValue();
        logger.debug("- col: '"+c.getName()+"': "+v);
      }

      // b) get columns by name
      logger.debug("emp");
      logger.debug("- emp id: "+cols.getIntegerValue(COL_NAME_EMPID, null));
      logger.debug("- dept: "+cols.getIntegerValue(COL_NAME_DEPTID, null));
      logger.debug("- firstName: "+cols.getStringValue(COL_NAME_FIRST_NAME, null));
      logger.debug("- lastName: "+cols.getStringValue(COL_NAME_LAST_NAME, null));
    
    } catch (ConnectionException e) {
      logger.error("failed to read from C*", e);
      throw new RuntimeException("failed to read from C*", e);
    }
  }
  
  public static void main(String[] args) {
    AstClient c = new AstClient();
    c.init();
    c.insert(222, 333, "Eric", "Cartman");
    c.read(222);
  }

}
