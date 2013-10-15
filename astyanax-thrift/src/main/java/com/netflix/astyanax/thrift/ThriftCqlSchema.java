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
