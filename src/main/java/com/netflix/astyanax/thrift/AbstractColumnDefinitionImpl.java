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

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;

import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.serializers.StringSerializer;

public class AbstractColumnDefinitionImpl implements ColumnDefinition {
	private final ColumnFamilyDefinition cfDef;
	private final ColumnDef columnDef;
	
	public AbstractColumnDefinitionImpl(ColumnFamilyDefinition cfDef, ColumnDef columnDef) {
		this.cfDef = cfDef;
		this.columnDef = columnDef;
		
		this.columnDef.setIndex_type(IndexType.KEYS);
	}
	
	@Override
	public ColumnDefinition setName(String name) {
		columnDef.setName(StringSerializer.get().toBytes(name));
		return this;
	}

	@Override
	public ColumnDefinition setValidationClass(String value) {
		columnDef.setValidation_class(value);
		return this;
	}
	
	@Override
	public ColumnFamilyDefinition endColumnDefinition() {
		return this.cfDef;
	}

}
