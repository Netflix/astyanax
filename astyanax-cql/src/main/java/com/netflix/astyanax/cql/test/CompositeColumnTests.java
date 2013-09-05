package com.netflix.astyanax.cql.test;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CompositeColumnTests extends KeyspaceTests {

	@BeforeClass
	public static void init() throws Exception {
		initContext();
	}
	

	public static class Population {
		
		@Component(ordinal=0) String city;
		@Component(ordinal=1) String state;
		@Component(ordinal=2) Integer zipcode;
		
		public Population() {
		}
		
		public String toString() {
			return "Pupulation [" + city + ", " + state + ", " + zipcode + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result + ((state == null) ? 0 : state.hashCode());
			result = prime * result + ((zipcode == null) ? 0 : zipcode.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null)return false;
			if (getClass() != obj.getClass()) return false;
			Population other = (Population) obj;
			boolean equal = true;
			equal &= (city != null) ? (city.equals(other.city)) : other.city == null; 
			equal &= (state != null) ? (state.equals(other.state)) : other.state == null; 
			equal &= (zipcode != null) ? (zipcode.equals(other.zipcode)) : other.zipcode == null;
			return equal;
		}
	}
	
	
	private static AnnotatedCompositeSerializer<Population> compSerializer = new AnnotatedCompositeSerializer<Population>(Population.class);

	private static ColumnFamily<String, Population> CF_POPULATION = 
			new ColumnFamily<String, Population>("population", StringSerializer.get(), compSerializer);
	
	//@Test
	public void testReadSingleRow() throws Exception {
		
		ColumnList<Population> result = keyspace.prepareQuery(CF_POPULATION).getRow("US").execute().getResult();
		
		Column<Population> p1 = result.getColumnByIndex(0);
		
		System.out.println("P1: " + p1.getName());
		System.out.println("P1: " + p1.getIntegerValue());
	}
	
	
	@Test
	public void testReadMultipleRows() throws Exception {
		
		Rows<String, Population> result = keyspace.prepareQuery(CF_POPULATION).getRowSlice("US").execute().getResult();
		Iterator<Row<String, Population>> iter = result.iterator();
		while (iter.hasNext()) {
			
			Row<String, Population> row = iter.next();
			System.out.println("Row key: " + row.getKey());
			ColumnList<Population> cols = row.getColumns();
			Column<Population> col = cols.getColumnByIndex(0);
			System.out.println("ColumnName: " + col.getName());
			//System.out.println("ColumnValue: " + col.getIntegerValue());
		}
	}
	
	
	
}
