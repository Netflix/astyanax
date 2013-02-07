package com.netflix.astyanax.entitystore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
final class NullableEntity {
	
	@Entity
	static class AllOptionalNestedEntity {

		@Column()
	    private String nullable;
	    
	    public String getNullable() {
			return nullable;
		}

		public void setNullable(String nullable) {
			this.nullable = nullable;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AllOptionalNestedEntity other = (AllOptionalNestedEntity) obj;
			if(((nullable == null && other.nullable == null) || (nullable != null && nullable.equals(other.nullable))))
				return true;
			else
				return false;
		}

        @Override
        public String toString() {
            return "AllOptionalNestedEntity [nullable=" + nullable + "]";
        }
	}
	
	@Entity
	static class AllMandatoryNestedEntity {

		@Column(nullable=false)
	    private String notnullable;
	    
	    public String getNotnullable() {
			return notnullable;
		}

		public void setNotnullable(String notnullable) {
			this.notnullable = notnullable;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AllMandatoryNestedEntity other = (AllMandatoryNestedEntity) obj;
			if(((notnullable == null && other.notnullable == null) || (notnullable != null && notnullable.equals(other.notnullable))))
				return true;
			else
				return false;
		}

        @Override
        public String toString() {
            return "AllMandatoryNestedEntity [notnullable=" + notnullable + "]";
        }
	}
	
    @Id
    private String id;
    
    @Column(nullable=false)
    private String notnullable;

	@Column()
    private String nullable;

	@Column()
	private AllOptionalNestedEntity notnullableAllOptionalNestedEntity;
	
	@Column()
	private AllOptionalNestedEntity nullableAllOptionalNestedEntity;
	
	@Column(nullable=false)
	private AllMandatoryNestedEntity notnullableAllMandatoryNestedEntity;
	
	@Column()
	private AllMandatoryNestedEntity nullableAllMandatoryNestedEntity;

	public NullableEntity() {
        
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNotnullable() {
		return notnullable;
	}

	public void setNotnullable(String notnullable) {
		this.notnullable = notnullable;
	}
    
    public String getNullable() {
		return nullable;
	}

	public void setNullable(String nullable) {
		this.nullable = nullable;
	}
	
	public AllOptionalNestedEntity getNotnullableAllOptionalNestedEntity() {
		return notnullableAllOptionalNestedEntity;
	}

	public void setNotnullableAllOptionalNestedEntity(
			AllOptionalNestedEntity notnullableAllOptionalNestedEntity) {
		this.notnullableAllOptionalNestedEntity = notnullableAllOptionalNestedEntity;
	}

	public AllOptionalNestedEntity getNullableAllOptionalNestedEntity() {
		return nullableAllOptionalNestedEntity;
	}

	public void setNullableAllOptionalNestedEntity(
			AllOptionalNestedEntity nullableAllOptionalNestedEntity) {
		this.nullableAllOptionalNestedEntity = nullableAllOptionalNestedEntity;
	}

	public AllMandatoryNestedEntity getNotnullableAllMandatoryNestedEntity() {
		return notnullableAllMandatoryNestedEntity;
	}

	public void setNotnullableAllMandatoryNestedEntity(
			AllMandatoryNestedEntity notnullableAllMandatoryNestedEntity) {
		this.notnullableAllMandatoryNestedEntity = notnullableAllMandatoryNestedEntity;
	}

	public AllMandatoryNestedEntity getNullableAllMandatoryNestedEntity() {
		return nullableAllMandatoryNestedEntity;
	}

	public void setNullableAllMandatoryNestedEntity(
			AllMandatoryNestedEntity nullableAllMandatoryNestedEntity) {
		this.nullableAllMandatoryNestedEntity = nullableAllMandatoryNestedEntity;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NullableEntity other = (NullableEntity) obj;
		if(id.equals(other.id) && 
				((notnullable == null && other.notnullable == null) || (notnullable != null && notnullable.equals(other.notnullable))) &&
				((nullable == null && other.nullable == null) || (notnullable != null && notnullable.equals(other.notnullable))) &&
				((notnullableAllOptionalNestedEntity == null && other.notnullableAllOptionalNestedEntity == null) 
						|| (notnullableAllOptionalNestedEntity != null && notnullableAllOptionalNestedEntity.equals(other.notnullableAllOptionalNestedEntity))) &&
				((nullableAllOptionalNestedEntity == null && other.nullableAllOptionalNestedEntity == null) 
						|| (nullableAllOptionalNestedEntity != null && nullableAllOptionalNestedEntity.equals(other.nullableAllOptionalNestedEntity))) &&
				((notnullableAllMandatoryNestedEntity == null && other.notnullableAllMandatoryNestedEntity == null) 
						|| (notnullableAllMandatoryNestedEntity != null && notnullableAllMandatoryNestedEntity.equals(other.notnullableAllMandatoryNestedEntity))) &&
				((nullableAllMandatoryNestedEntity == null && other.nullableAllMandatoryNestedEntity == null) 
						|| (nullableAllMandatoryNestedEntity != null && nullableAllMandatoryNestedEntity.equals(other.nullableAllMandatoryNestedEntity)))
				)
			return true;
		else
			return false;
	}

    @Override
    public String toString() {
        return "NullableEntity [id=" + id + ", notnullable=" + notnullable + ", nullable=" + nullable
                + ", notnullableAllOptionalNestedEntity=" + notnullableAllOptionalNestedEntity
                + ", nullableAllOptionalNestedEntity=" + nullableAllOptionalNestedEntity + ", notnullableAllMandatoryNestedEntity="
                + notnullableAllMandatoryNestedEntity + ", nullableAllMandatoryNestedEntity=" + nullableAllMandatoryNestedEntity
                + "]";
    }

}
