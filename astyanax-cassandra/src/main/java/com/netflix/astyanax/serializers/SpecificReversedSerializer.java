package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SpecificReversedSerializer extends ReversedSerializer {

	private String reversedTypeName;
	private ComparatorType comparatorType;
	
	public SpecificReversedSerializer(ReversedType type) {
		Preconditions.checkNotNull(type);
		AbstractType<?> compType = type.baseType;
		reversedTypeName = compType.toString();
		if ( reversedTypeName.startsWith( "org.apache.cassandra.db.marshal." ) ) {
			reversedTypeName = reversedTypeName.substring( "org.apache.cassandra.db.marshal.".length() );
		}		
		this.comparatorType = ComparatorType.getByClassName(reversedTypeName);
	}

	@Override
	public ByteBuffer fromString(String string) {
		return this.comparatorType.getSerializer().fromString(string);
	}

	@Override
	public String getString(ByteBuffer byteBuffer) {
		return this.comparatorType.getSerializer().getString(byteBuffer);
	}	
}
