package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.netflix.astyanax.serializers.StringSerializer;

public class ThriftCql3Query<K,C> extends AbstractThriftCqlQuery<K, C> {

    ThriftCql3Query(ThriftColumnFamilyQueryImpl<K, C> cfQuery, String cql) {
        super(cfQuery, cql);
    }

    @Override
    protected org.apache.cassandra.thrift.CqlPreparedResult prepare_cql_query(Client client) throws InvalidRequestException, TException {
        return client.prepare_cql3_query(StringSerializer.get().toByteBuffer(cql), Compression.NONE);
    }
    
    @Override
    protected org.apache.cassandra.thrift.CqlResult execute_prepared_cql_query(Client client, int id, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
        return client.execute_prepared_cql3_query(id, values, ThriftConverter.ToThriftConsistencyLevel(cl));
    }
    
    @Override
    protected org.apache.cassandra.thrift.CqlResult execute_cql_query(Client client) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
        return client.execute_cql3_query(
            StringSerializer.get().toByteBuffer(cql),
            useCompression ? Compression.GZIP : Compression.NONE,
                    ThriftConverter.ToThriftConsistencyLevel(cl));
    }

}
