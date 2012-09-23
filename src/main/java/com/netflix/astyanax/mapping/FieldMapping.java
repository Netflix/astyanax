package com.netflix.astyanax.mapping;

import java.lang.reflect.Field;

public class FieldMapping {

    private Field field;

    private Integer ttl = null;

    public FieldMapping() {

    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }
}
