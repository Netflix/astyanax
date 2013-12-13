package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;


public class CFRowSliceQueryGen {

	protected final Session session;
	protected final String keyspace; 
	protected final CqlColumnFamilyDefinitionImpl cfDef;

	protected final String partitionKeyCol;
	protected final String[] allPrimayKeyCols;
	protected final List<ColumnDefinition> clusteringKeyCols;
	protected final List<ColumnDefinition> regularCols;
	
	protected boolean isCompositeColumn; 
	
	protected static final String BIND_MARKER = "?";
	
	public CFRowSliceQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.session = session;

		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		allPrimayKeyCols = cfDef.getAllPkColNames();
		clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		regularCols = cfDef.getRegularColumnDefinitionList();

		isCompositeColumn = (clusteringKeyCols.size() > 1);
	}

	protected abstract class RowSliceQueryCache {

		private final AtomicReference<PreparedStatement> cachedStatement = new AtomicReference<PreparedStatement>(null);

		public abstract Callable<RegularStatement> getQueryGen(CqlRowSliceQueryImpl<?,?> rowSliceQuery);

		public BoundStatement getBoundStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

			PreparedStatement pStatement = getPreparedStatement(rowSliceQuery, useCaching);
			return bindValues(pStatement, rowSliceQuery);
		}

		public abstract BoundStatement bindValues(PreparedStatement pStatement, CqlRowSliceQueryImpl<?,?> rowSliceQuery);

		public PreparedStatement getPreparedStatement(CqlRowSliceQueryImpl<?,?> rowSliceQuery, boolean useCaching) {

			PreparedStatement pStatement = null;

			if (useCaching) {
				pStatement = cachedStatement.get();
			}

			if (pStatement == null) {
				try {
					RegularStatement query = getQueryGen(rowSliceQuery).call();
					System.out.println("query " + query.getQueryString());
					pStatement = session.prepare(query.getQueryString());
					System.out.println("pStatement " + pStatement.getQueryString());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			if (useCaching && cachedStatement.get() == null) {
				cachedStatement.set(pStatement);
			}
			return pStatement;
		}
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
					stmt.and(eq(columnName, op.getValue()));
					componentIndex++;
					break;
				case LESS_THAN :
					stmt.and(lt(columnName, op.getValue()));
					break;
				case LESS_THAN_EQUALS:
					stmt.and(lte(columnName, op.getValue()));
					break;
				case GREATER_THAN:
					stmt.and(gt(columnName, op.getValue()));
					break;
				case GREATER_THAN_EQUALS:
					stmt.and(gte(columnName, op.getValue()));
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
	
}
