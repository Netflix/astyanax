package com.netflix.astyanax.cql.reads;

import java.util.Iterator;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCopier;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlKeyspaceImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.CqlColumnImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

/**
 * Impl for {@link RowCopier}
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlRowCopier<K,C> implements RowCopier<K,C> {

	private boolean useOriginalTimestamp = false;
	
	private final RowQuery<K,C> rowQuery;
	private final ColumnFamily<K,C> cf;
	private final K rowKey; 
	private final KeyspaceContext ksContext;
	
	public CqlRowCopier(ColumnFamily<K,C> cf, K rowKey, RowQuery<K,C> query, KeyspaceContext ksContext) {
		this.cf = cf;
		this.rowKey = rowKey;
		this.rowQuery = query;
		this.ksContext = ksContext;
	}
	
	@Override
	public OperationResult<Void> execute() throws ConnectionException {
		return getMutationBatch().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Void>> executeAsync() throws ConnectionException {
		return getMutationBatch().executeAsync();
	}

	@Override
	public RowCopier<K, C> withOriginalTimestamp(boolean useTimestamp) {
		this.useOriginalTimestamp = useTimestamp;
		return this;
	}
	
	private MutationBatch getMutationBatch() throws ConnectionException {
		
		ColumnList<C> columnList = rowQuery.execute().getResult();
		
		CqlKeyspaceImpl ksImpl = new CqlKeyspaceImpl(ksContext);
		
		MutationBatch mBatch = ksImpl.prepareMutationBatch();
		CqlColumnListMutationImpl<K,C> colListMutation = (CqlColumnListMutationImpl<K, C>)mBatch.withRow(cf, rowKey);
		
		Iterator<Column<C>> iter = columnList.iterator();

		boolean first = true;
		
		while(iter.hasNext()) {
			
			CqlColumnImpl<C> col = (CqlColumnImpl<C>) iter.next();
			
			if (first && useOriginalTimestamp) {
				colListMutation.setTimestamp(col.getTimestamp());
				first = false;
			}
			
			colListMutation.putColumnWithGenericValue(col.getName(), col.getGenericValue(), null);
		}
		
		return mBatch;
	}
}
