package com.netflix.astyanax.cql.test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.test.SessionEvent;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class ClickStreamTests extends KeyspaceTests {

	public static AnnotatedCompositeSerializer<SessionEvent> SE_SERIALIZER 
	= new AnnotatedCompositeSerializer<SessionEvent>(SessionEvent.class);

	public static ColumnFamily<String, SessionEvent> CF_CLICK_STREAM = 
			ColumnFamily.newColumnFamily("ClickStream", StringSerializer.get(), SE_SERIALIZER);

	@BeforeClass
	public static void init() throws Exception {
		initContext();

		keyspace.createColumnFamily(CF_CLICK_STREAM, null);
		CF_CLICK_STREAM.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_CLICK_STREAM);
	}

	@Test
	public void testClickStream() throws Exception {

		MutationBatch m = keyspace.prepareMutationBatch();
		String userId = "UserId";

		List<UUID> uuids = new ArrayList<UUID>();
		for (int j = 0; j < 10; j++) {
			uuids.add(TimeUUIDUtils.getTimeUUID(j));
		}

		long timeCounter = 0;
		for (int i = 0; i < 10; i++) {
			String sessionId = "Session" + i;

			for (int j = 0; j < 10; j++) {
				m.withRow(CF_CLICK_STREAM, userId).putColumn(
						new SessionEvent(sessionId, uuids.get(j)),
						Long.toString(timeCounter), null);
				timeCounter++;
			}
		}

		m.execute();


		OperationResult<ColumnList<SessionEvent>> result;

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER.buildRange()
						.greaterThanEquals("Session3")
						.lessThanEquals("Session5").build())
						.execute();

		Assert.assertEquals(30, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER.buildRange()
						.greaterThanEquals("Session3")
						.lessThan("Session5").build()).execute();
		Assert.assertEquals(20, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER.buildRange().greaterThan("Session3")
						.lessThanEquals("Session5").build())
						.execute();
		Assert.assertEquals(20, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER.buildRange().greaterThan("Session3")
						.lessThan("Session5").build()).execute();
		Assert.assertEquals(10, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER
						.buildRange()
						.withPrefix("Session3")
						.greaterThanEquals(uuids.get(2))
						.lessThanEquals(uuids.get(8))
						.build()).execute();

		Assert.assertEquals(7, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER
						.buildRange()
						.withPrefix("Session3")
						.greaterThanEquals(
								TimeUUIDUtils.getTimeUUID(2))
								.lessThan(
										TimeUUIDUtils.getTimeUUID(8))
										.build()).execute();

		Assert.assertEquals(6, result.getResult().size());


		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER
						.buildRange()
						.withPrefix("Session3")
						.greaterThan(
								TimeUUIDUtils.getTimeUUID(2))
								.lessThanEquals(
										TimeUUIDUtils.getTimeUUID(8))
										.build()).execute();

		Assert.assertEquals(6, result.getResult().size());

		result = keyspace
				.prepareQuery(CF_CLICK_STREAM)
				.getKey(userId)
				.withColumnRange(
						SE_SERIALIZER
						.buildRange()
						.withPrefix("Session3")
						.greaterThan(
								TimeUUIDUtils.getTimeUUID(2))
								.lessThan(
										TimeUUIDUtils.getTimeUUID(8))
										.build()).execute();

		Assert.assertEquals(5, result.getResult().size());
	}
}

