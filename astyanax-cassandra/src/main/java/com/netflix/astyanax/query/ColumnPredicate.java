package com.netflix.astyanax.query;

import com.netflix.astyanax.model.Equality;

public class ColumnPredicate {
    private String   name;
    private Equality op;
    private Object   value;
    
    public String getName() {
        return name;
    }

    public Equality getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    public ColumnPredicate setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnPredicate setOp(Equality op) {
        this.op = op;
        return this;
    }

    public ColumnPredicate setValue(Object value) {
        this.value = value;
        return this;
    }
    
    @Override
    public String toString() {
        return "ColumnPredicate [name=" + name + ", op=" + op + ", value="
                + value + "]";
    }

    
}
