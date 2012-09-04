package com.netflix.astyanax.connectionpool;

public interface Endpoint {
    String getHost();

    String getDatacenter();

    String getRack();
}
