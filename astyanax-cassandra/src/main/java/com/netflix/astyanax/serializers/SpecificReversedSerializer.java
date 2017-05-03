/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SpecificReversedSerializer extends ReversedSerializer {

	private String reversedTypeName;
	private final ComparatorType reversedComparatorType;
	
	public SpecificReversedSerializer(ReversedType type) {
		Preconditions.checkNotNull(type);
		AbstractType<?> compType = type.baseType;
		reversedTypeName = compType.toString();
		if ( reversedTypeName.startsWith( "org.apache.cassandra.db.marshal." ) ) {
			reversedTypeName = reversedTypeName.substring( "org.apache.cassandra.db.marshal.".length() );
		}		
		this.reversedComparatorType = ComparatorType.getByClassName(reversedTypeName);
	}

	@Override
	public ByteBuffer fromString(String string) {
		return this.reversedComparatorType.getSerializer().fromString(string);
	}

	@Override
	public String getString(ByteBuffer byteBuffer) {
		return this.reversedComparatorType.getSerializer().getString(byteBuffer);
	}	
}
