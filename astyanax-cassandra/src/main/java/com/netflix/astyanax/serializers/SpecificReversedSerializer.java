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
