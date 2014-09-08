package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlPreparedStatement;

public abstract class AbstractThriftCqlPreparedStatement implements CqlPreparedStatement {

    @Override
    public <V> CqlPreparedStatement withByteBufferValue(V value, Serializer<V> serializer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withValue(ByteBuffer value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withValues(List<ByteBuffer> value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withStringValue(String value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withIntegerValue(Integer value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withBooleanValue(Boolean value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withDoubleValue(Double value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withLongValue(Long value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withFloatValue(Float value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withShortValue(Short value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CqlPreparedStatement withUUIDValue(UUID value) {
        // TODO Auto-generated method stub
        return null;
    }

}
