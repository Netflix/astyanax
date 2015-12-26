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

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Equality;

public abstract class CompositeRangeBuilder implements ByteBufferRange {
    private ByteBufferOutputStream start = new ByteBufferOutputStream();
    private ByteBufferOutputStream end = new ByteBufferOutputStream();
    private int limit = Integer.MAX_VALUE;
    private boolean reversed = false;
    private boolean lockComponent = false;

    private List<RangeQueryRecord> records = new ArrayList<RangeQueryRecord>();
    
    protected void nextComponent() {
    	getNextComponent();
    	// flip to a new record, which is for a new component
    	records.add(new RangeQueryRecord());
    }

    abstract protected void getNextComponent();

    abstract protected void append(ByteBufferOutputStream out, Object value, Equality equality);

    public CompositeRangeBuilder withPrefix(Object object) {
        if (lockComponent) {
            throw new IllegalStateException("Prefix cannot be added once equality has been specified");
        }
        append(start, object, Equality.EQUAL);
        append(end, object, Equality.EQUAL);
        
        getLastRecord().addQueryOp(object, Equality.EQUAL);
        nextComponent();
        
        return this;
    }

    public CompositeRangeBuilder limit(int count) {
        this.limit = count;
        return this;
    }

    public CompositeRangeBuilder reverse() {
        reversed = true;
        ByteBufferOutputStream temp = start;
        start = end;
        end = temp;
        return this;
    }

    public CompositeRangeBuilder greaterThan(Object value) {
        lockComponent = true;
        append(start, value, Equality.GREATER_THAN);
        getLastRecord().addQueryOp(value, Equality.GREATER_THAN);

        return this;
    }

    public CompositeRangeBuilder greaterThanEquals(Object value) {
        lockComponent = true;
        append(start, value, Equality.GREATER_THAN_EQUALS);
        getLastRecord().addQueryOp(value, Equality.GREATER_THAN_EQUALS);

        return this;
    }

    public CompositeRangeBuilder lessThan(Object value) {
        lockComponent = true;
        append(end, value, Equality.LESS_THAN);
        getLastRecord().addQueryOp(value, Equality.LESS_THAN);
        return this;
    }

    public CompositeRangeBuilder lessThanEquals(Object value) {
        lockComponent = true;
        append(end, value, Equality.LESS_THAN_EQUALS);
        getLastRecord().addQueryOp(value, Equality.LESS_THAN_EQUALS);
        return this;
    }
    
    private RangeQueryRecord getLastRecord() {
    	
    	if (records.size() == 0) {
    		RangeQueryRecord record = new RangeQueryRecord();
    		records.add(record);
    	}
    	
    	return records.get(records.size()-1);
    }

    @Override
    @Deprecated
    public ByteBuffer getStart() {
        return start.getByteBuffer();
    }

    @Override
    @Deprecated
    public ByteBuffer getEnd() {
        return end.getByteBuffer();
    }

    @Override
    @Deprecated
    public boolean isReversed() {
        return reversed;
    }

    @Override
    @Deprecated
    public int getLimit() {
        return limit;
    }

    public CompositeByteBufferRange build() {
    	return new CompositeByteBufferRange(start, end, limit, reversed, records);
    }
    
    public static class RangeQueryRecord {
    	
    	private List<RangeQueryOp> ops = new ArrayList<RangeQueryOp>();

    	public RangeQueryRecord() {
    		
    	}
    	
    	public void addQueryOp(Object value, Equality operator) {
    		add(new RangeQueryOp(value, operator));
    	}
    	
    	public void add(RangeQueryOp rangeOp) {
    		ops.add(rangeOp);
    	}
    	
    	public List<RangeQueryOp> getOps() {
    		return ops;
    	}
    }

    public static class RangeQueryOp {
		
		private final Object value;
		private final Equality operator;
		
		public RangeQueryOp(Object value, Equality operator) {
			this.value = value;
			this.operator = operator;
		}
		
		public Object getValue() {
			return value;
		}
		public Equality getOperator() {
			return operator;
		}
	}
    
    public static class CompositeByteBufferRange implements ByteBufferRange {
    	
        private final ByteBufferOutputStream start;
        private final ByteBufferOutputStream end;
        private int limit;
        private boolean reversed;
        private final List<RangeQueryRecord> records;

        private CompositeByteBufferRange(ByteBufferOutputStream rangeStart, ByteBufferOutputStream rangeEnd, 
        		int rangeLimit, boolean rangeReversed, 
        		List<RangeQueryRecord> rangeRecords) {
        	this.start = rangeStart;
        	this.end = rangeEnd;
        	this.limit = rangeLimit;
        	this.reversed = rangeReversed;
        	this.records = rangeRecords;
        }
        
        public CompositeByteBufferRange(int rangeLimit, boolean rangeReversed, List<RangeQueryRecord> rangeRecords) {
        	// This is only meant to be called by internally
        	this.start = null;
        	this.end = null;
        	this.limit = rangeLimit;
        	this.reversed = rangeReversed;
        	this.records = rangeRecords;
        }

    	 @Override
         public ByteBuffer getStart() {
             return start.getByteBuffer();
         }

         @Override
         public ByteBuffer getEnd() {
             return end.getByteBuffer();
         }

         @Override
         public boolean isReversed() {
             return reversed;
         }

         @Override
         public int getLimit() {
             return limit;
         }
         
         public List<RangeQueryRecord> getRecords() {
        	 return records;
         }
    }
}
