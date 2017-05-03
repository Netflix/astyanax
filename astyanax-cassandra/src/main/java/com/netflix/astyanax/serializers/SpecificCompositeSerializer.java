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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

import com.google.common.base.Preconditions;

public class SpecificCompositeSerializer extends CompositeSerializer {
	private final CompositeType type;
	private List<String> comparators;

	public SpecificCompositeSerializer(CompositeType type) {
		Preconditions.checkNotNull(type);
		this.type = type;
		comparators = new ArrayList<String>( type.types.size() );
		for ( AbstractType<?> compType : type.types ) {
			String typeName = compType.toString();
			if ( typeName.startsWith( "org.apache.cassandra.db.marshal." ) ) {
				typeName = typeName.substring( "org.apache.cassandra.db.marshal.".length() );
			}
			comparators.add( typeName );
		}
	}

	@Override
	public ByteBuffer fromString(String string) {
		return type.fromString(string);
	}

	@Override
	public String getString(ByteBuffer byteBuffer) {
		return type.getString(byteBuffer);
	}

	@Override
	public List<String> getComparators() {
		return comparators;
	}
}
