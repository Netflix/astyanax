/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
