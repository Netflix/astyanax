/**
 * 
 */
package com.netflix.astyanax.serializers;


import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;


/**
 * @author Todd Nine
 * 
 */
public class CompositeSerializer extends AbstractSerializer<Composite> {

  @Override
  public ByteBuffer toByteBuffer(Composite obj) {

    return obj.serialize();
  }

  @Override
  public Composite fromByteBuffer(ByteBuffer byteBuffer) {

    Composite composite = new Composite();
    composite.deserialize(byteBuffer);

    return composite;

  }

  @Override
  public ComparatorType getComparatorType() {
    return ComparatorType.COMPOSITETYPE;
  }

}
