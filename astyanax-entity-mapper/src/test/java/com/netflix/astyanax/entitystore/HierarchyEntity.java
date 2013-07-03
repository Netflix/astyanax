package com.netflix.astyanax.entitystore;

import java.math.BigInteger;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

@MappedSuperclass
public class HierarchyEntity {

	@Id
	Long id;
	@Column
	String mappedSuperClassColumn;

	public static class NonMappedEntity extends HierarchyEntity {
		@Column
		String nonMappedColumn;
	}

	@Entity
	public static class ChildOfNonMappedEntity extends NonMappedEntity {
		@Column
		String childColumn;
	}

	@Entity
	public static class MappedEntity extends HierarchyEntity {
		@Column
		String mappedColumn;
	}

	@Entity
	public static class ChildOfMappedEntity extends MappedEntity {
		@Column
		String childColumn;
	}

	@Entity
	public static class OverrideColumnEntity extends MappedEntity {
		@Column
		BigInteger mappedColumn;
	}

	@Entity
	public static class OverrideIdEntity extends MappedEntity {
		@Id
		BigInteger bigintId;
	}
}
