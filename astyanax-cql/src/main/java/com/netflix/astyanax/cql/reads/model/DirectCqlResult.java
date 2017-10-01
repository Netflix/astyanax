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
package com.netflix.astyanax.cql.reads.model;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

/**
 * Impl for {@link CqlResult} that parses the {@link ResultSet} from java driver.
 * Note that the class does not interpret the column family definition but instead simply
 * parses each CQL column returned in the result set as an Astyanax column. 
 * Hence this class is applicable in instances where the table is considered to be a flat CQL table.  
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class DirectCqlResult<K, C> implements CqlResult<K, C> {

	private Long number = null;
	private CqlRowListImpl<K, C> rows; 
	
	public DirectCqlResult(List<Row> rows, ColumnFamily<K,C> cf) {
		
		List<com.netflix.astyanax.model.Row<K,C>> rowList = new ArrayList<com.netflix.astyanax.model.Row<K,C>>();
		
		for (Row row : rows) {
			rowList.add(getAstyanaxRow(row, cf));
		}
		this.rows = new CqlRowListImpl<K, C>(rowList);
	}

	public DirectCqlResult(Long number) {
		this.number = number;
	}
	
	@Override
	public Rows<K, C> getRows() {
		return this.rows;
	}

	@Override
	public int getNumber() {
		return number.intValue();
	}

	@Override
	public boolean hasRows() {
		return rows != null && rows.size() > 0;
	}

	@Override
	public boolean hasNumber() {
		return this.number != null;
	}

	private com.netflix.astyanax.model.Row<K, C> getAstyanaxRow(Row row, ColumnFamily<K,C> cf) {
		CqlRowImpl<K,C> rowImpl = new CqlRowImpl<K,C>(getAstyanaxRowKey(row, cf), getAstyanaxColumnList(row), cf);
		return rowImpl;
	}
	
	private K getAstyanaxRowKey(Row row, ColumnFamily<K,C> cf) {
		
		Serializer<K> keySerializer = cf.getKeySerializer();
		return (K) CqlTypeMapping.getDynamicColumn(row, keySerializer, 0, cf);
	}
	
	private CqlColumnListImpl<C> getAstyanaxColumnList(Row row) {
		
		List<CqlColumnImpl<C>> list = new ArrayList<CqlColumnImpl<C>>();

		List<Definition> colDefs  = row.getColumnDefinitions().asList();
		int index = 0;
		
		for (Definition colDef : colDefs) {
			C columnName = (C) colDef.getName();
			list.add(new CqlColumnImpl<C>(columnName, row, index, colDef));
			index++;
		}
		
		return new CqlColumnListImpl<C>(list);
	}
}
