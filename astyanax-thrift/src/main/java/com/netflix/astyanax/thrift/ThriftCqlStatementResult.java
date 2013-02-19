package com.netflix.astyanax.thrift;

import org.apache.cassandra.thrift.CqlResult;

import com.netflix.astyanax.cql.CqlSchema;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.model.ThriftCqlRowsImpl;

public class ThriftCqlStatementResult implements CqlStatementResult {
    private CqlResult result;
    
    public ThriftCqlStatementResult(CqlResult result) {
        this.result = result;
    }

    @Override
    public long asCount() {
        throw new RuntimeException("Not supported yet");
    }
    
    @Override
    public <K, C> Rows<K, C> getRows(ColumnFamily<K, C> columnFamily) {
        if (!result.isSetRows()) 
            throw new RuntimeException("CQL reponse doesn't contain rows");
        
        return new ThriftCqlRowsImpl<K, C>(result.getRows(), columnFamily.getKeySerializer(), columnFamily.getColumnSerializer());
    }
    
    @Override
    public CqlSchema getSchema() {
        return new ThriftCqlSchema(result.getSchema());
    }

//    switch (res.getType()) {
//    case ROWS:
//        return new ThriftCqlResultImpl<K, C>(new ThriftCqlRowsImpl<K, C>(res.getRows(),
//                columnFamily.getKeySerializer(), columnFamily.getColumnSerializer()));
//    case INT:
//        return new ThriftCqlResultImpl<K, C>(res.getNum());
//        
//    default:
//        return null;
//    }

}
