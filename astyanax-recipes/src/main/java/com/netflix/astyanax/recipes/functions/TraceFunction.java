package com.netflix.astyanax.recipes.functions;

import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Function;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;

/**
 * Simple function to trace the contents
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class TraceFunction<K,C> implements Function<Row<K,C>, Boolean> {

    public static class Builder<K,C> {
        private OutputStream out = System.out;
        private boolean showColumns = false;
        
        public Builder(ColumnFamily<K,C> columnFamily) {
        }
        
        public Builder<K,C> withOutputStream(OutputStream out) {
            this.out = out;
            return this;
        }
        
        public Builder<K,C> withShowColumns(boolean showColumns) {
            this.showColumns = showColumns;
            return this;
        }
        
        public TraceFunction<K,C> build() {
            return new TraceFunction<K,C>(this);
        }
    }

    public static <K, C> Builder<K,C> builder(ColumnFamily<K,C> columnFamily) {
        return new Builder<K,C>(columnFamily);
    }
    
    private final OutputStream out;
    private final boolean showColumns;
    
    private TraceFunction(Builder<K,C> builder) {
        this.out                    = builder.out;
        this.showColumns            = builder.showColumns;
    }
    
    @Override
    public synchronized Boolean apply(Row<K, C> row) {
        long size = 0;
        for (Column<C> column : row.getColumns()) {
            size += column.getRawName().limit() + column.getByteBufferValue().limit();
        }
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(String.format("- row: '%s' size: '%dl' count: '%dl' \n", row.getKey(), size, row.getColumns().size()));
        if (showColumns) {
            for (Column<C> column : row.getColumns()) {
                sb.append(String.format("  '%s' (ts='%dl', ttl='%d')\n", column.getName(), column.getTimestamp(), column.getTtl()));
            }
        }
        try {
            out.write(sb.toString().getBytes());
        } catch (IOException e) {
        }
        return true;
    }
    

}
