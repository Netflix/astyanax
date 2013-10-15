package com.netflix.astyanax.entitystore;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.model.Equality;

/**
 * Yet another attempt at simplifying how composite columns are built
 * 
 * @author elandau
 *
 */
public class SimpleCompositeBuilder {
    private final static int  COMPONENT_OVERHEAD = 3;
    
    private int bufferSize;
    private ByteBuffer bb;
    private boolean hasControl = true;
    private Equality lastEquality = Equality.EQUAL;
    private final Equality finalEquality;
    
    public SimpleCompositeBuilder(int bufferSize, Equality finalEquality) {
        bb = ByteBuffer.allocate(bufferSize);
        this.finalEquality = finalEquality;
    }
    
    public void add(ByteBuffer cb, Equality control) {
        addWithoutControl(cb);
        addControl(control);
    }
    
    public void addWithoutControl(ByteBuffer cb) {
        Preconditions.checkState(lastEquality == Equality.EQUAL, "Cannot extend composite since non equality control already set");
        
        if (cb == null) {
            cb = ByteBuffer.allocate(0);
        }

        if (cb.limit() + COMPONENT_OVERHEAD > bb.remaining()) {
            int exponent = (int) Math.ceil(Math.log((double) (cb.limit() + COMPONENT_OVERHEAD + bb.limit())) / Math.log(2));
            bufferSize = (int) Math.pow(2, exponent);
            ByteBuffer temp = ByteBuffer.allocate(bufferSize);
            bb.flip();
            temp.put(bb);
            bb = temp;
        }
        
        if (!hasControl) {
            addControl(Equality.EQUAL);
        }
        else if (bb.position() > 0) {
            bb.position(bb.position() - 1);
            bb.put(Equality.EQUAL.toByte());
        }

        // Write the data: <length><data>
        bb.putShort((short) cb.remaining());
        bb.put(cb.slice());
        hasControl = false;
    }
    
    public void addControl(Equality control) {
        Preconditions.checkState(!hasControl, "Control byte already set");
        Preconditions.checkState(lastEquality == Equality.EQUAL, "Cannot extend composite since non equality control already set");
        hasControl = true;
        bb.put(control.toByte());
    }
    
    public boolean hasControl() {
        return hasControl;
    }
    
    public ByteBuffer get() {
        if (!hasControl) 
            addControl(this.finalEquality);
        
        ByteBuffer ret = bb.duplicate();
        ret.flip();
        return ret;
    }
}
