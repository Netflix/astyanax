package com.netflix.astyanax.connectionpool;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

/**
 * A host predicate that filters the result of node discovery to only hosts within a specified data center.
 */
public class DataCenterFilter implements Predicate<TokenRange.EndpointDetails> {
    private final String dataCenter;

    public DataCenterFilter(String dataCenter) {
        this.dataCenter = Preconditions.checkNotNull(dataCenter);
    }

    @Override
    public boolean apply(TokenRange.EndpointDetails endpoint) {
        return dataCenter.equals(endpoint.getDataCenter());
    }
}
