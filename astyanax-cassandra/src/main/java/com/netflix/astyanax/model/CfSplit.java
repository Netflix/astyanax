package com.netflix.astyanax.model;

/**
 * Represents a contiguous range of rows, bounded by tokens.
 */
public interface CfSplit {
    String getStartToken();

    String getEndToken();

    long getRowCount();
}
