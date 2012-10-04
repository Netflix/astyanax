package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.TokenRange;

public class EndpointDetailsImpl implements TokenRange.EndpointDetails {
    private final String host;
    private final String dataCenter;
    private final String rack;

    public EndpointDetailsImpl(String host, String dataCenter, String rack) {
        this.host = host;
        this.dataCenter = dataCenter;
        this.rack = rack;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getDataCenter() {
        return dataCenter;
    }

    @Override
    public String getRack() {
        return rack;
    }
}
