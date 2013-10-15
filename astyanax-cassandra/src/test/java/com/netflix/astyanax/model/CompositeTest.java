package com.netflix.astyanax.model;

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.netflix.astyanax.serializers.ByteBufferOutputStream;

public class CompositeTest {
    @Test
    public void testByteBufferOutputStream() throws Exception {
        ByteBufferOutputStream out = new ByteBufferOutputStream();
        
        int length = 0;
        for (int i = 0; i < 300; i++) {
            length += i;
            out.write(StringUtils.repeat("*",  i).getBytes());
        }
        
        ByteBuffer buffer = out.getByteBuffer();
        Assert.assertEquals(buffer.capacity(), length);
    }
}
