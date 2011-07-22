package com.netflix.astyanax.model;

import java.nio.ByteBuffer;

public interface RangeEndpoint {
	ByteBuffer toBytes();
	
	RangeEndpoint append(Object value, Equality equality);
}
