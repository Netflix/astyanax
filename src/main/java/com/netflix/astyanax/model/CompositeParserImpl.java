package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

public class CompositeParserImpl implements CompositeParser {
	private final Composite composite;
	private int position = 0;
	
	public CompositeParserImpl(ByteBuffer bb) {
		this.composite = Composite.fromByteBuffer(bb);
	}
	
	@Override
	public String readString() {
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long readLong() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer readInteger() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean readBoolean() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UUID readUUID() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(Serializer<T> serializer) {
		Object obj = this.composite.get(position, serializer);
		position++;
		return (T)obj;
	}

}
