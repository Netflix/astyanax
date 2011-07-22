package com.netflix.astyanax.model;

import java.nio.ByteBuffer;

/**
 * Interface to get a raw byte buffer range.  Subclasses of ByteBufferRange
 * are usually builders that simplify their creation.
 * @author elandau
 *
 */
public interface ByteBufferRange {
	ByteBuffer getStart();
	ByteBuffer getEnd();
	boolean isReversed();
	int getLimit();
}
