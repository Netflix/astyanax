package com.netflix.astyanax.entitystore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class SimpleEntity {
    @Id
    private String id;
    
    @Column
    private String column;
    
    public SimpleEntity() {
        
    }
    
    public SimpleEntity(String id, String column) {
        this.id = id;
        this.column = column;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "SimpleEntity [id=" + id + ", column=" + column + "]";
    }
    
    
}
