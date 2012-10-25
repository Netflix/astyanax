package com.netflix.astyanax.ddl.impl;

import com.netflix.astyanax.ddl.SchemaChangeResult;

public class SchemaChangeResponseImpl implements SchemaChangeResult {
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
