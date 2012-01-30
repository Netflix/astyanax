package com.netflix.astyanax.util;

import com.netflix.astyanax.model.Rows;

public interface RowsWriter {
    public void write(Rows<?, ?> rows) throws Exception;
}
