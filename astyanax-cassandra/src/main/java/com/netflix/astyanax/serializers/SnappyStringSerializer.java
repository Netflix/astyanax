package com.netflix.astyanax.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.codec.binary.StringUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class SnappyStringSerializer extends AbstractSerializer<String> {

    private static final SnappyStringSerializer instance = new SnappyStringSerializer();

    public static SnappyStringSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(String obj) {
        if (obj == null) {
            return null;
        }
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SnappyOutputStream snappy;
        try {
            snappy = new SnappyOutputStream(out);
            snappy.write(StringUtils.getBytesUtf8(obj));
            snappy.close();
            return ByteBuffer.wrap(out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Error compressing column data", e);
        }        
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        
        SnappyInputStream snappy = null;
        ByteArrayOutputStream baos = null;
        try {
            ByteBuffer dup = byteBuffer.duplicate();
            snappy = new SnappyInputStream(
                    new ByteArrayInputStream(dup.array(), 0,
                            dup.limit()));
            
            baos = new ByteArrayOutputStream();
            for (int value = 0; value != -1;) {
                value = snappy.read();
                if (value != -1) {
                    baos.write(value);
                }
            }
            snappy.close();
            baos.close();
            return StringUtils.newStringUtf8(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Error decompressing column data", e);
        } finally {
            if (snappy != null) {
                try {
                    snappy.close();
                } catch (IOException e) {
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                }
            }
        }
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.BYTESTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return UTF8Type.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
            return UTF8Type.instance.getString(byteBuffer);
            }
}
