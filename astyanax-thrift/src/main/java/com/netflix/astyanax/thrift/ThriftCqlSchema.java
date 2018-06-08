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
import java.util.Map.Entry;

import org.apache.cassandra.thrift.CqlMetadata;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.CqlSchema;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.StringSerializer;

public class ThriftCqlSchema implements CqlSchema {
    private final CqlMetadata schema;
    
    public ThriftCqlSchema(CqlMetadata schema) {
        this.schema = schema;
        
        System.out.println("Name:  " + schema.getDefault_name_type());
        System.out.println("Value: " + schema.getDefault_value_type());
        
        for (Entry<ByteBuffer, String> type : schema.getName_types().entrySet()) {
            Serializer serializer = ComparatorType.valueOf(type.getValue().toUpperCase()).getSerializer();
            System.out.println("Name: " + type.getValue() + " = " + serializer.getString(type.getKey()));
        }
        
        for (Entry<ByteBuffer, String> value : schema.getValue_types().entrySet()) {
            Serializer serializer = StringSerializer.get(); // ComparatorType.valueOf(value.getValue().toUpperCase()).getSerializer();
            System.out.println("Type: " + value.getValue() + " = " + ((value.getKey() == null) ? "null" : serializer.getString(value.getKey())));
        }
    }
}
