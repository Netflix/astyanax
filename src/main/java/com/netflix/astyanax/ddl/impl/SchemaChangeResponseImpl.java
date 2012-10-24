package com.netflix.astyanax.ddl.impl;

import com.netflix.astyanax.ddl.SchemaChangeResponse;

public class SchemaChangeResponseImpl implements SchemaChangeResponse {
    private String schemaId;
    
    @Override
    public String getSchemaId() {
        return schemaId;
    }
    
    public SchemaChangeResponseImpl setSchemaId(String id) {
        this.schemaId = id;
        return this;
    }

}
