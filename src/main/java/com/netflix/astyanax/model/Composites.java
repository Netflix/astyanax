package com.netflix.astyanax.model;

import java.nio.ByteBuffer;

public class Composites {
	public static CompositeBuilder newDynamicCompositeBuilder() {
		return new CompositeBuilderImpl(new DynamicComposite());
	}
	
	public static CompositeBuilder newCompositeBuilder() {
		return new CompositeBuilderImpl(new Composite());
	}
	
	public static CompositeParser newCompositeParser(ByteBuffer bb) {
		return new CompositeParserImpl(bb);
	}
	
	public static CompositeParser newDynamicCompositeParser(ByteBuffer bb) {
		return null;
	}
}
