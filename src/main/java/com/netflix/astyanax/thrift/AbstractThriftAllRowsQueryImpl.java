package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.AllRowsQuery;

public abstract class AbstractThriftAllRowsQueryImpl<K, C> implements AllRowsQuery<K, C> {

    protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.RANGE_ALL);
    // protected KeyRange range = new
    // KeyRange().setCount(100).setStart_token("0").setEnd_token("0");
    private int blockSize = 100;
    private ColumnFamily<K, C> columnFamily;
    private boolean repeatLastToken = true;
    private ExceptionCallback exceptionCallback;
    private Integer nThreads;
    
    public AbstractThriftAllRowsQueryImpl(ColumnFamily<K, C> columnFamily) {
        this.columnFamily = columnFamily;
    }

    public AllRowsQuery<K, C> setExceptionCallback(ExceptionCallback cb) {
        exceptionCallback = cb;
        return this;
    }

    protected ExceptionCallback getExceptionCallback() {
        return this.exceptionCallback;
    }

    @Override
    public AllRowsQuery<K, C> setThreadCount(int numberOfThreads) {
        setConcurrencyLevel(numberOfThreads);
        return this;
    }
    
    @Override
    public AllRowsQuery<K, C> setConcurrencyLevel(int numberOfThreads) {
        this.nThreads = numberOfThreads;
        return this;
    }
    
    @Override
    public AllRowsQuery<K, C> withColumnSlice(C... columns) {
        if (columns != null)
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(Arrays.asList(columns)))
                    .setSlice_rangeIsSet(false);
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnSlice(Collection<C> columns) {
        if (columns != null)
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(columns)).setSlice_rangeIsSet(
                    false);
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
        predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(), startColumn,
                endColumn, reversed, count));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
        predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
        if (slice.getColumns() != null) {
            predicate.setColumn_names(columnFamily.getColumnSerializer().toBytesList(slice.getColumns()))
                    .setSlice_rangeIsSet(false);
        }
        else {
            predicate.setSlice_range(ThriftUtils.createSliceRange(columnFamily.getColumnSerializer(),
                    slice.getStartColumn(), slice.getEndColumn(), slice.getReversed(), slice.getLimit()));
        }
        return this;
    }

    @Override
    public AllRowsQuery<K, C> withColumnRange(ByteBufferRange range) {
        predicate.setSlice_range(new SliceRange().setStart(range.getStart()).setFinish(range.getEnd())
                .setCount(range.getLimit()).setReversed(range.isReversed()));
        return this;
    }

    @Override
    public AllRowsQuery<K, C> setBlockSize(int blockSize) {
        return setRowLimit(blockSize);
    }

    @Override
    public AllRowsQuery<K, C> setRowLimit(int rowLimit) {
        this.blockSize = rowLimit;
        return this;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public AllRowsQuery<K, C> setRepeatLastToken(boolean repeatLastToken) {
        this.repeatLastToken = repeatLastToken;
        return this;
    }

    public boolean getRepeatLastToken() {
        return this.repeatLastToken;
    }

    protected Integer getConcurrencyLevel() {
        return this.nThreads;
    }
    
    protected abstract List<org.apache.cassandra.thrift.KeySlice> getNextBlock(KeyRange range);
}
