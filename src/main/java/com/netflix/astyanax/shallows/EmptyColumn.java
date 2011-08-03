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
package com.netflix.astyanax.shallows;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class EmptyColumn<C> implements Column<C> {

	@Override
	public C getName() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V> V getValue(Serializer<V> valSer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getStringValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isParentColumn() {
		return false;
	}

	@Override
	public int getIntegerValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getLongValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getByteArrayValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getBooleanValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBuffer getByteBufferValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getDateValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public UUID getUUIDValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getTimestamp() {
		throw new UnsupportedOperationException();
	}
}
