package com.netflix.astyanax.serializers;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility to collect data written to an {@link OutputStream} in
 * {@link ByteBuffer}s.
 * 
 * Originally from org.apache.avro.util.ByteBufferOutputStream, moved into
 * Hector and added getByteBuffer to return single ByteBuffer from contents.
 */
public class ByteBufferOutputStream extends OutputStream {
    public static final int INIT_BUFFER_SIZE = 64;
    public static final int MAX_BUFFER_SIZE = 8192;
    
    private List<ByteBuffer> buffers;
    private int bufferSize = INIT_BUFFER_SIZE;
    
    public ByteBufferOutputStream() {
        reset();
    }

    /** Returns all data written and resets the stream to be empty. */
    public List<ByteBuffer> getBufferList() {
        List<ByteBuffer> result = buffers;
        reset();
        for (ByteBuffer buffer : result) {
            buffer.flip();
        }
        return result;
    }

    public ByteBuffer getByteBuffer() {
        List<ByteBuffer> list = getBufferList();
        // if there's just one bytebuffer in list, return it
        if (list.size() == 1) {
            return list.get(0);
        }
        int size = 0;
        for (ByteBuffer buffer : list) {
            size += buffer.remaining();
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        for (ByteBuffer buffer : list) {
            result.put(buffer);
        }
        return (ByteBuffer) result.rewind();
    }

    /** Prepend a list of ByteBuffers to this stream. */
    public void prepend(List<ByteBuffer> lists) {
        for (ByteBuffer buffer : lists) {
            buffer.position(buffer.limit());
        }
        buffers.addAll(0, lists);
    }

    /** Append a list of ByteBuffers to this stream. */
    public void append(List<ByteBuffer> lists) {
        for (ByteBuffer buffer : lists) {
            buffer.position(buffer.limit());
        }
        buffers.addAll(lists);
    }

    public void reset() {
        buffers = new LinkedList<ByteBuffer>();
        bufferSize = INIT_BUFFER_SIZE;
        buffers.add(ByteBuffer.allocate(bufferSize));
    }

    private ByteBuffer getBufferWithCapacity(int capacity) {
        ByteBuffer buffer = buffers.get(buffers.size() - 1);
        if (buffer.remaining() < capacity) {
            buffer = ByteBuffer.allocate(bufferSize);
            buffers.add(buffer);
        }
        return buffer;
    }

    @Override
    public void write(int b) {
        ByteBuffer buffer = getBufferWithCapacity(1);
        buffer.put((byte) b);
    }

    public void writeShort(short value) {
        ByteBuffer buffer = getBufferWithCapacity(2);
        buffer.putShort(value);
    }

    public void writeChar(char value) {
        ByteBuffer buffer = getBufferWithCapacity(2);
        buffer.putChar(value);
    }

    public void writeInt(int value) {
        ByteBuffer buffer = getBufferWithCapacity(4);
        buffer.putInt(value);
    }

    public void writeFloat(float value) {
        ByteBuffer buffer = getBufferWithCapacity(4);
        buffer.putFloat(value);
    }

    public void writeLong(long value) {
        ByteBuffer buffer = getBufferWithCapacity(8);
        buffer.putLong(value);
    }

    public void writeDouble(double value) {
        ByteBuffer buffer = getBufferWithCapacity(8);
        buffer.putDouble(value);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ByteBuffer buffer = buffers.get(buffers.size() - 1);
        int remaining = buffer.remaining();
        while (len > remaining) {
            buffer.put(b, off, remaining);
            len -= remaining;
            off += remaining;
            
            bufferSize *= 2;
            if (bufferSize > MAX_BUFFER_SIZE)
                bufferSize = MAX_BUFFER_SIZE;
            
            buffer = ByteBuffer.allocate(bufferSize);
            buffers.add(buffer);
            remaining = buffer.remaining();
        }
        buffer.put(b, off, len);
    }

    /** Add a buffer to the output without copying, if possible. */
    public void write(ByteBuffer buffer) {
        if (buffer.remaining() < bufferSize) {
            write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        else { // append w/o copying bytes
            ByteBuffer dup = buffer.duplicate();
            dup.position(buffer.limit()); // ready for flip
            buffers.add(dup);
        }
    }
}
