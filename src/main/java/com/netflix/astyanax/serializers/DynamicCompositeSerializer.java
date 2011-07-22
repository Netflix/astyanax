/**
 * 
 */
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.DynamicComposite;


/**
 * @author Todd Nine
 * 
 */
public class DynamicCompositeSerializer extends
    AbstractSerializer<DynamicComposite> {

  @Override
  public ByteBuffer toByteBuffer(DynamicComposite obj) {

    return obj.serialize();
  }

  @Override
  public DynamicComposite fromByteBuffer(ByteBuffer byteBuffer) {

    return DynamicComposite.fromByteBuffer(byteBuffer);

  }

  @Override
  public ComparatorType getComparatorType() {
    return ComparatorType.DYNAMICCOMPOSITETYPE;
  }

}
