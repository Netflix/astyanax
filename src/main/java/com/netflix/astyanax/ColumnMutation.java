package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.model.ConsistencyLevel;

public interface ColumnMutation {
	ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	Execution<Void> putValue(String value, Integer ttl);

	Execution<Void> putValue(byte[] value, Integer ttl);

	Execution<Void> putValue(int value, Integer ttl);

	Execution<Void> putValue(long value, Integer ttl);

	Execution<Void> putValue(boolean value, Integer ttl);

	Execution<Void> putValue(ByteBuffer value, Integer ttl);

	Execution<Void> putValue(Date value, Integer ttl);

	Execution<Void> putValue(double value, Integer ttl);

	Execution<Void> putValue(UUID value, Integer ttl);
	
	Execution<Void> putEmptyColumn(Integer ttl);
	
	Execution<Void> incrementCounterColumn(long amount);
	
	Execution<Void> deleteColumn();
}
