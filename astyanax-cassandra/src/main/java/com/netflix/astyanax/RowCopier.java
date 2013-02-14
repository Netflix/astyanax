package com.netflix.astyanax;

public interface RowCopier<K, C> extends Execution<Void> {
    RowCopier<K,C> withOriginalTimestamp(boolean useOriginalTimestamp);
}
