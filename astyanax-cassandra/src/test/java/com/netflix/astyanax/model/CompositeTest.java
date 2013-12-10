package com.netflix.astyanax.model;


import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.serializers.ByteBufferOutputStream;

import junit.framework.Assert;

import static org.junit.Assert.assertEquals;


public class CompositeTest {
    @Test
    public void testByteBufferOutputStream() throws Exception {
        ByteBufferOutputStream out = new ByteBufferOutputStream();

        int length = 0;
        for ( int i = 0; i < 300; i++ ) {
            length += i;
            out.write( StringUtils.repeat( "*", i ).getBytes() );
        }

        ByteBuffer buffer = out.getByteBuffer();
        Assert.assertEquals( buffer.capacity(), length );
    }


    /**
     * Test the I/O of using a static composite works correctly
     */
    @Test
    public void compositeSerializesPrimitives() {
        final CompositeBuilder builder = Composites.newCompositeBuilder();

        final String string = "test";
        final UUID uuid = UUID.randomUUID();
        final boolean bool = true;
        final Integer integer = 10;
        final Long longval = 20l;


        builder.addString( string );
        builder.addUUID( uuid );
        builder.addBoolean( bool );
        builder.addInteger( integer );
        builder.addLong( longval );


        final CompositeParser parser = Composites.newCompositeParser( builder.build() );

        //now read back
        assertEquals( string, parser.readString() );
        assertEquals( uuid, parser.readUUID() );
        assertEquals( bool, parser.readBoolean() );
        assertEquals( integer, parser.readInteger() );
        assertEquals( longval, parser.readLong() );
    }

    /**
        * Test the index out of bounds on the read
        */
       @Test(expected = IndexOutOfBoundsException.class)
       public void compositeOutofBounds() {
           final CompositeBuilder builder = Composites.newCompositeBuilder();

           final String string = "test";

           builder.addString( string );


           final CompositeParser parser = Composites.newCompositeParser( builder.build() );

           //now read back
           assertEquals( string, parser.readString() );

           //read beyond available elements.  Should throw IndexOutOfBoundsException
           parser.readString();
       }
}
