package com.netflix.astyanax.query;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.CqlResult;

public class PreparedQueryTests {
	@Test
	public void testAdditionOfValues(){
		AbstractPreparedCqlQuery<?,?> s = new AbstractPreparedCqlQuery<Object, Object>() {

			@Override
			public OperationResult<CqlResult<Object, Object>> execute()
					throws ConnectionException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ListenableFuture<OperationResult<CqlResult<Object, Object>>> executeAsync()
					throws ConnectionException {
				throw new UnsupportedOperationException();
			}
			
		};
		assertTrue("New query object should contain no values",s.getValues().isEmpty());
		List<ByteBuffer> expectedValues = new LinkedList<ByteBuffer>();
		expectedValues.add(ByteBuffer.wrap("hello".getBytes()));
		expectedValues.add(ByteBuffer.wrap("world".getBytes()));
		s.withValues(expectedValues);
		List<ByteBuffer> actualValues = s.getValues();
		String errorMessage = "failed to add values to the query object";
		assertEquals(errorMessage, expectedValues.size(),actualValues.size());
		for(int i = 0 ; i < actualValues.size(); ++i){
			assertArrayEquals(errorMessage,expectedValues.get(i).array(),actualValues.get(i).array());
		}
	}

}
