package com.netflix.astyanax.model;


import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class DynamicCompositeTest {


    @Test
    public void testComposite() {
        DynamicComposite dc = new DynamicComposite();
        for ( char ch = 'A'; ch < 'Z'; ch++ ) {
            dc.addComponent( Character.toString( ch ), StringSerializer.get() );
        }
    }


    @Test
    public void testReversedSerialization() {

        AsciiSerializer asciiSerializer = AsciiSerializer.get();
        BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();

        IntegerSerializer integerSerializer = IntegerSerializer.get();
        LongSerializer longSerializer = LongSerializer.get();

        StringSerializer stringSerializer = StringSerializer.get();

        UUIDSerializer uuidSerializer = UUIDSerializer.get();


        DynamicComposite dc = new DynamicComposite();

        final String string = "test";
        final byte[] bytes = new byte[] { 0x00 };
        final int intValue = 1;
        final long longValue = 1l;
        final UUID uuid = UUID.randomUUID();


        dc.addComponent( string, asciiSerializer, getReversed( asciiSerializer ) );

        dc.addComponent( bytes, bytesArraySerializer, getReversed( bytesArraySerializer ) );

        dc.addComponent( intValue, integerSerializer, getReversed( integerSerializer ) );

        dc.addComponent( longValue, longSerializer, getReversed( longSerializer ) );

        dc.addComponent( string, stringSerializer, getReversed( stringSerializer ) );

        dc.addComponent( uuid, uuidSerializer, getReversed( uuidSerializer ) );

        //serialize to bytes
        ByteBuffer buff = dc.serialize();

        //de-serialize
        DynamicComposite read = DynamicComposite.fromByteBuffer( buff );

        assertEquals(6, read.size());

        assertEquals(string, read.getComponent( 0 ).getValue( asciiSerializer ));

        assertArrayEquals( bytes, ( byte[] ) read.getComponent( 1 ).getValue( bytesArraySerializer ) );

        assertEquals(intValue, read.getComponent( 2 ).getValue( integerSerializer ));

        assertEquals(longValue, read.getComponent( 3 ).getValue( longSerializer ));

        assertEquals(string, read.getComponent( 4 ).getValue( stringSerializer ));

        assertEquals(uuid, read.getComponent( 5 ).getValue( uuidSerializer ));
    }


    private String getReversed( AbstractSerializer serializer ) {
        return serializer.getComparatorType().getTypeName() + "(reversed=true)";
    }
}
