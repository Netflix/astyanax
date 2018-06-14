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
