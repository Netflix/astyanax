package com.netflix.astyanax.cql.test;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.uniqueness.NotUniqueException;
import com.netflix.astyanax.recipes.uniqueness.RowUniquenessConstraint;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class RowUniquenessConstraintTest extends KeyspaceTests {

	public static ColumnFamily<Long, String> CF_UNIQUE_CONSTRAINT = ColumnFamily
			.newColumnFamily(
					"cfunique", 
					LongSerializer.get(),
					StringSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_UNIQUE_CONSTRAINT, null);
		CF_UNIQUE_CONSTRAINT.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_UNIQUE_CONSTRAINT);
	}


	Supplier<String> UniqueColumnSupplier = new Supplier<String>() {

		@Override
		public String get() {
			return UUID.randomUUID().toString();
		}
	};

	@Test
	public void testUnique() throws Exception {

		RowUniquenessConstraint<Long, String> unique = 
				new RowUniquenessConstraint<Long, String>(keyspace, CF_UNIQUE_CONSTRAINT, 1L, UniqueColumnSupplier)
				.withConsistencyLevel(ConsistencyLevel.CL_ONE);

		unique.acquire();

		try { 
			unique = new RowUniquenessConstraint<Long, String>(keyspace, CF_UNIQUE_CONSTRAINT, 1L, UniqueColumnSupplier)
					.withConsistencyLevel(ConsistencyLevel.CL_ONE);
			unique.acquire();
			Assert.fail("Should have gotten a non-unique ex");
		} catch (NotUniqueException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testUniqueAndRelease() throws Exception {

		RowUniquenessConstraint<Long, String> unique = 
				new RowUniquenessConstraint<Long, String>(keyspace, CF_UNIQUE_CONSTRAINT, 2L, UniqueColumnSupplier)
				.withConsistencyLevel(ConsistencyLevel.CL_ONE);

		unique.acquire();
		unique.release();

		unique = new RowUniquenessConstraint<Long, String>(keyspace, CF_UNIQUE_CONSTRAINT, 2L, UniqueColumnSupplier)
				.withConsistencyLevel(ConsistencyLevel.CL_ONE);
		unique.acquire();
	}

	@Test
	public void testUniquenessWithCustomMutation() throws Exception {

		ColumnList<String> result = keyspace.prepareQuery(CF_UNIQUE_CONSTRAINT).getRow(10L).execute().getResult();
		Assert.assertTrue(result.isEmpty());

		RowUniquenessConstraint<Long, String> unique = 
				new RowUniquenessConstraint<Long, String>(keyspace, CF_UNIQUE_CONSTRAINT, 3L, UniqueColumnSupplier)
				.withConsistencyLevel(ConsistencyLevel.CL_ONE);

		unique.acquireAndApplyMutation(new Function<MutationBatch, Boolean>() {
			public Boolean apply(MutationBatch input) {

				input.withRow(CF_UNIQUE_CONSTRAINT, 10L).putEmptyColumn("MyCustomColumn", null);
				return true;
			}
		});

		result = keyspace.prepareQuery(CF_UNIQUE_CONSTRAINT).getRow(10L).execute().getResult();
		Assert.assertFalse(result.isEmpty());
	}
}
