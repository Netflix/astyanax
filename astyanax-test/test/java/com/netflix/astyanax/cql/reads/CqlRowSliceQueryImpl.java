package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListIterator;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;

/**
 * Impl for {@link RowSliceQuery} interface. 
 * 
 * Just like {@link CqlRowQueryImpl} this class only manages the context for the row slice query, but does not construct the actual
 * CQL query itself. For more details on how the actual query is constructed see classes 
 * {@link CFRowKeysQueryGen} and {@link CFRowRangeQueryGen}
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<K,C> cfContext;

	private final CqlRowSlice<K> rowSlice;
	private CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	
	private CompositeByteBufferRange compositeRange = null;

	private final boolean isPaginating; 
	private boolean useCaching = false;
	
	public enum RowSliceQueryType {
		RowKeys, RowRange
	}

	public enum ColumnSliceQueryType {
		AllColumns, ColumnSet, ColumnRange; 
	}
	
	private final RowSliceQueryType rowQueryType;
	private ColumnSliceQueryType colQueryType = ColumnSliceQueryType.AllColumns;
	
	public CqlRowSliceQueryImpl(KeyspaceContext ksCtx, CFQueryContext<K,C> cfCtx, CqlRowSlice<K> rSlice, boolean useCaching) {
		this(ksCtx, cfCtx, rSlice, true, useCaching);
	}

	public CqlRowSliceQueryImpl(KeyspaceContext ksCtx, CFQueryContext<K,C> cfCtx, CqlRowSlice<K> rSlice, boolean condition, boolean useCaching) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowSlice = rSlice;
		this.isPaginating = condition;
		this.rowQueryType = (rowSlice.isRangeQuery()) ? RowSliceQueryType.RowRange : RowSliceQueryType.RowKeys;
		this.useCaching = useCaching;
	}

	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		return new InternalRowQueryExecutionImpl(this).execute();
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl(this).executeAsync();
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(C... columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
		colQueryType = ColumnSliceQueryType.ColumnSet;
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		colQueryType = ColumnSliceQueryType.ColumnRange;
		this.columnSlice = new CqlColumnSlice<C>(new CqlRangeBuilder<C>()
				.setColumn("column1")
				.setStart(startColumn)
				.setEnd(endColumn)
				.setReversed(reversed)
				.setLimit(count)
				.build());
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int limit) {
		colQueryType = ColumnSliceQueryType.ColumnRange;
		Serializer<C> colSerializer = cfContext.getColumnFamily().getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@SuppressWarnings("unchecked")
	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {

		colQueryType = ColumnSliceQueryType.ColumnRange;

		if (range instanceof CompositeByteBufferRange) {
			this.compositeRange = (CompositeByteBufferRange) range;

		} else if (range instanceof CompositeRangeBuilder) {
			this.compositeRange = ((CompositeRangeBuilder)range).build();

		} else if (range instanceof CqlRangeImpl) {
			this.columnSlice.setCqlRange((CqlRangeImpl<C>) range);
		} else {
			return this.withColumnRange(range.getStart(), range.getEnd(), range.isReversed(), range.getLimit());
		}
		return this;
	}

	@Override
	public RowSliceColumnCountQuery<K> getColumnCounts() {
		Statement query = new InternalRowQueryExecutionImpl(this).getQuery();
		return new CqlRowSliceColumnCountQueryImpl<K>(ksContext, cfContext, query);
	}

	@SuppressWarnings("unchecked")
	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<Rows<K, C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		private final CqlRowSliceQueryImpl<K, C> rowSliceQuery; 
		
		public InternalRowQueryExecutionImpl(CqlRowSliceQueryImpl<K, C> rSliceQuery) {
			super(ksContext, cfContext);
			this.rowSliceQuery = rSliceQuery;
		}
		
		public InternalRowQueryExecutionImpl(KeyspaceContext ksContext, CFQueryContext<?, ?> cfContext) {
			super(ksContext, cfContext);
			this.rowSliceQuery = null;
		}

		@Override
		public Statement getQuery() {
			return cfDef.getRowQueryGenerator().getQueryStatement(rowSliceQuery, useCaching);
		}

		@Override
		public Rows<K, C> parseResultSet(ResultSet rs) throws NotFoundException {
			if (!isPaginating) {
				List<com.datastax.driver.core.Row> rows = rs.all();
				if (rows == null || rows.isEmpty()) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListImpl<K, C>(rows, (ColumnFamily<K, C>) cf);
			} else {
				if (rs == null) {
					return new CqlRowListImpl<K, C>();
				}
				return new CqlRowListIterator<K, C>(rs, (ColumnFamily<K, C>) cf);
			}
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
	}

	public CqlRowSlice<K> getRowSlice() {
		return rowSlice;
	}
	
	public CqlColumnSlice<C> getColumnSlice() {
		return columnSlice;
	}

	public CompositeByteBufferRange getCompositeRange() {
		return compositeRange;
	}
	
	public ColumnSliceQueryType getColQueryType() {
		return colQueryType;
	}

	public RowSliceQueryType getRowQueryType() {
		return rowQueryType;
	}
}


