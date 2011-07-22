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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.CounterColumn;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.InvalidOperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ThriftCounterSuperColumnImpl<C> implements Column<C> {
	private final List<CounterColumn> columns;
	private final C name;

	public ThriftCounterSuperColumnImpl(C name, List<CounterColumn> columns) {
		this.name = name;
		this.columns = columns;
	}
	
	@Override
	public C getName() {
		return name;
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new InvalidOperationException(
				"UnsupportedOperationException \'" + this.name+ "\' has no value");
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public long getLongValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		return new ThriftCounterColumnListImpl<C2>(this.columns, ser);
	}

	@Override
	public boolean isParentColumn() {
		return true;
	}

	@Override
	public byte[] getByteArrayValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public boolean getBooleanValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public Date getDateValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}

	@Override
	public UUID getUUIDValue() {
		throw new UnsupportedOperationException(
				"CounterSuperColumn \'" + this.name+ "\' has no value");
	}
}
