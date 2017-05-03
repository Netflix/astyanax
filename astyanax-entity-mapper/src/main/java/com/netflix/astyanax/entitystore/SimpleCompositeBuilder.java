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
