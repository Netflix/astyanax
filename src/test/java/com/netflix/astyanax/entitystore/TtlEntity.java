package com.netflix.astyanax.entitystore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@TTL(2)
public class TtlEntity {
    @Id
    private String id;
    
    @Column
    private String column;
    
    public TtlEntity() {
        
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TtlEntity other = (TtlEntity) obj;
		if(id.equals(other.id) && column.equals(other.column))
			return true;
		else
			return false;
	}

    @Override
    public String toString() {
        return "SimpleEntity [id=" + id + ", column=" + column + "]";
    }
}
