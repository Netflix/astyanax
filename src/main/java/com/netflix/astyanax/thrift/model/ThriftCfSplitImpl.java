package com.netflix.astyanax.thrift.model;

import com.netflix.astyanax.model.CfSplit;

public class ThriftCfSplitImpl implements CfSplit {
    private final String startToken;
    private final String endToken;
    private final long rowCount;

    public ThriftCfSplitImpl(String startToken, String endToken, long rowCount) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.rowCount = rowCount;
    }

    @Override
    public String getStartToken() {
        return startToken;
    }

    @Override
    public String getEndToken() {
        return endToken;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "CfSplitImpl [startToken=" + startToken + ", endToken=" + endToken + ", rowCount=" + rowCount + "]";
    }
}
