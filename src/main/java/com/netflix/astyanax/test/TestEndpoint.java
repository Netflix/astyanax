package com.netflix.astyanax.test;

import com.netflix.astyanax.connectionpool.Endpoint;

public class TestEndpoint implements Endpoint {

    private final String host;
    private final String datacenter;
    private final String rack;

    public TestEndpoint(String host, String datacenter, String rack) {
        this.host = host;
        this.datacenter = datacenter;
        this.rack = rack;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public String getDatacenter() {
        return this.datacenter;
    }

    @Override
    public String getRack() {
        return this.rack;
    }
}
