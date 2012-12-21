package com.netflix.astyanax.serializers;

import com.netflix.astyanax.mapping.StringCoercible;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * A StringCoercibleSerializer translates the byte[] to and from string using utf-8
 * encoding.
 * 
 * @author Dave Johnson
 * 
 */
public final class StringCoercibleSerializer extends AbstractSerializer<StringCoercible> {

    private static final String UTF_8 = "UTF-8";
    private static final StringCoercibleSerializer instance = new StringCoercibleSerializer();
    private static final Charset charset = Charset.forName(UTF_8);

    public static StringCoercibleSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(StringCoercible obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.serializeToString().getBytes(charset));
    }

    public StringCoercible fromByteBuffer(ByteBuffer byteBuffer, Class clazz) {
        if (byteBuffer == null) {
            return null;
        }
        String s = charset.decode(byteBuffer).toString();
		try {
			StringCoercible obj = (StringCoercible)clazz.newInstance();
			obj.initFromString(s);
			return obj;
		} catch (Exception ex) {
			return null;
		}
    }

	@Override
    public StringCoercible fromByteBuffer(ByteBuffer byteBuffer) {
		return this.fromByteBuffer(byteBuffer, null);
	}

	public StringCoercible fromBytes(byte[] bytes, Class clazz) {
        return fromByteBuffer(ByteBuffer.wrap(bytes), clazz);
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.UTF8TYPE;
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
