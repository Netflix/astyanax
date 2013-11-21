package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SpecificReversedSerializer extends ReversedSerializer {

	private String reversedTypeName;
	
	public SpecificReversedSerializer(ReversedType type) {
		Preconditions.checkNotNull(type);
		AbstractType<?> compType = type.baseType;
		reversedTypeName = compType.toString();
		if ( reversedTypeName.startsWith( "org.apache.cassandra.db.marshal." ) ) {
			reversedTypeName = reversedTypeName.substring( "org.apache.cassandra.db.marshal.".length() );
		}		
	}

	@Override
	public ByteBuffer fromString(String string) {
		return ComparatorType.getByClassName(reversedTypeName).getSerializer().fromString(string);
	}

	@Override
	public String getString(ByteBuffer byteBuffer) {
		return ComparatorType.getByClassName(reversedTypeName).getSerializer().getString(byteBuffer);
	}	
}
