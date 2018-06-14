/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
