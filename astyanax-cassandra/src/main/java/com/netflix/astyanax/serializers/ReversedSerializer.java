package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

public class ReversedSerializer<T> extends AbstractSerializer<T>{

	@SuppressWarnings("rawtypes")
	private static final ReversedSerializer instance = new ReversedSerializer() ;
	
	@SuppressWarnings("unchecked")
	public static <T> ReversedSerializer<T> get()
	{		
		return instance;		
	}
		
	@Override
	public ByteBuffer toByteBuffer(T obj) {
		if (obj == null)
			return null;
		
		return SerializerTypeInferer.getSerializer(obj).toByteBuffer(obj);		
	}

	@Override
	public T fromByteBuffer(ByteBuffer byteBuffer) {
        throw new RuntimeException(
                "ReversedSerializer.fromByteBuffer() Not Implemented.");
	}
	
    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.REVERSEDTYPE;
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new RuntimeException(
                "ReversedSerializer.getNext() Not implemented.");
    }

}
