package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

/**
 * Base class that contains the utilities for generating queries for read operations via the 
 * {@link RowSliceQuery} class. 
 * 
 * Note that this class is just a place holder for some useful generic utilities. 
 * See {@link CFRowKeysQueryGen} and {@link CFRowRangeQueryGen} which are the 2 extending classes 
 * for functionality that actually supports the queries. 
 * 
 * @author poberai
 */
public class CFRowSliceQueryGen {

	// Thread safe reference to the underlying session object. We need the session object to be able to "prepare" query statements
	protected final AtomicReference<Session> sessionRef = new AtomicReference<Session>(null);
	// the keyspace being queried. Used for all the underlying queries being generated
	protected final String keyspace; 
	// the cf definition which helps extending classes construct the right query as per the schema
	protected final CqlColumnFamilyDefinitionImpl cfDef;

	// Other useful derivatives of the cf definition that are frequently used by query generators
	protected final String partitionKeyCol;
	protected final String[] allPrimayKeyCols;
	protected final List<ColumnDefinition> clusteringKeyCols;
	protected final List<ColumnDefinition> regularCols;
	
	// Condition tracking whether the underlying schema uses composite columns. This is imp since it influences how 
	// a single Column (composite column) can be decomposed into it's individual components that form different parts of the query.
	protected boolean isCompositeColumn; 
	
	// bind marker for generating the prepared statements
	protected static final String BIND_MARKER = "?";
	
	public CFRowSliceQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);

		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		regularCols = cfDef.getRegularColumnDefinitionList();

		isCompositeColumn = (clusteringKeyCols.size() > 1);
	}
	
	/**
	 * 
	 *   SOME BASIC UTILITY METHODS USED BY ALL THE ROW SLICE QUERY GENERATORS
	 */
	
	protected Select selectAllColumnsFromKeyspaceAndCF() {

		Select.Selection select = QueryBuilder.select();
		for (int i=0; i<allPrimayKeyCols.length; i++) {
			select.column(allPrimayKeyCols[i]);
		}

		for (ColumnDefinition colDef : regularCols) {
			String colName = colDef.getName();
			select.column(colName).ttl(colName).writeTime(colName);
		}
		return select.from(keyspace, cfDef.getName());
	}
	
	protected Where addWhereClauseForColumnRange(Where where, CqlColumnSlice<?> columnSlice) {

		String clusteringKeyCol = clusteringKeyCols.get(0).getName();

		if (!columnSlice.isRangeQuery()) {
			return where;
		}
		if (columnSlice.getStartColumn() != null) {
			where.and(gte(clusteringKeyCol, columnSlice.getStartColumn()));
		}
		if (columnSlice.getEndColumn() != null) {
			where.and(lte(clusteringKeyCol, columnSlice.getEndColumn()));
		}

		if (columnSlice.getReversed()) {
			where.orderBy(desc(clusteringKeyCol));
		}

		if (columnSlice.getLimit() != -1) {
			where.limit(columnSlice.getLimit());
		}

		return where;
	}
	
	protected void bindWhereClauseForColumnRange(List<Object> values, CqlColumnSlice<?> columnSlice) {

		if (!columnSlice.isRangeQuery()) {
			return;
		}
		if (columnSlice.getStartColumn() != null) {
			values.add(columnSlice.getStartColumn());
		}
		if (columnSlice.getEndColumn() != null) {
			values.add(columnSlice.getEndColumn());
		}

		if (columnSlice.getLimit() != -1) {
			values.add(columnSlice.getLimit());
		}

		return;
	}
	

	protected Where addWhereClauseForCompositeColumnRange(Where stmt, CompositeByteBufferRange compositeRange) {

		List<RangeQueryRecord> records = compositeRange.getRecords();
		int componentIndex = 0; 

		for (RangeQueryRecord record : records) {

			for (RangeQueryOp op : record.getOps()) {

				String columnName = clusteringKeyCols.get(componentIndex).getName();

				switch (op.getOperator()) {

				case EQUAL:
					stmt.and(eq(columnName, BIND_MARKER));
					componentIndex++;
					break;
				case LESS_THAN :
					stmt.and(lt(columnName, BIND_MARKER));
					break;
				case LESS_THAN_EQUALS:
					stmt.and(lte(columnName, BIND_MARKER));
					break;
				case GREATER_THAN:
					stmt.and(gt(columnName, BIND_MARKER));
					break;
				case GREATER_THAN_EQUALS:
					stmt.and(gte(columnName, BIND_MARKER));
					break;
				default:
					throw new RuntimeException("Cannot recognize operator: " + op.getOperator().name());
				}; // end of switch stmt
			} // end of inner for for ops for each range query record
		}
		return stmt;
	}

	protected void bindWhereClauseForCompositeColumnRange(List<Object> values, CompositeByteBufferRange compositeRange) {

		List<RangeQueryRecord> records = compositeRange.getRecords();

		for (RangeQueryRecord record : records) {
			for (RangeQueryOp op : record.getOps()) {
				values.add(op.getValue());
			}
		}
		return;
	}
	
	protected Object[] bindMarkerArray(int n) {
		
		Object[] arr = new Object[n];
		for (int i=0; i<n; i++) {
			arr[i] = BIND_MARKER;
		}
		return arr;
	}
}
