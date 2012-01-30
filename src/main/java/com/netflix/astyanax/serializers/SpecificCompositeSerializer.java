package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.CompositeType;

public class SpecificCompositeSerializer extends CompositeSerializer {
    private final CompositeType type;
    
    public SpecificCompositeSerializer(CompositeType type) {
        this.type = type;
    }
    
    @Override
    public ByteBuffer fromString(String string) {
        return type.fromString(string);
    }
    
    @Override
    public String getString(ByteBuffer byteBuffer) {
        return type.getString(byteBuffer);
    }

}
