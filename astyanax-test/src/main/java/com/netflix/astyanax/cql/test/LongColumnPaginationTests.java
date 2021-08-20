/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class LongColumnPaginationTests extends KeyspaceTests {
	
	public static final ColumnFamily<String, Long> CF_LONGCOLUMN = ColumnFamily
            .newColumnFamily(
                    "LongColumn1", 
                    StringSerializer.get(),
                    LongSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_LONGCOLUMN, null);
		CF_LONGCOLUMN.describe(keyspace);
	}

    @AfterClass
	public static void teardown() throws Exception {
    	keyspace.dropColumnFamily(CF_LONGCOLUMN);
	}

    @Test
    public void paginateLongColumns() throws Exception {

        String rowKey = "A";
        MutationBatch m = keyspace.prepareMutationBatch();
        ColumnListMutation<Long> cfmLong = m.withRow(CF_LONGCOLUMN, rowKey);
        for (Long l = -10L; l < 10L; l++) {
            cfmLong.putEmptyColumn(l, null);
        }
        cfmLong.putEmptyColumn(Long.MAX_VALUE, null);
        m.execute();
        
        // READ BACK WITH PAGINATION
        Long column = Long.MIN_VALUE;
        ColumnList<Long> columns;
        int pageSize = 10;
        RowQuery<String, Long> query = keyspace
        		.prepareQuery(CF_LONGCOLUMN)
        		.getKey("A")
        		.autoPaginate(true)
        		.withColumnRange(
        				new CqlRangeBuilder<Long>()
        				.setStart(column)
        				.setFetchSize(pageSize).build());

        int pageCount = 0;
        int colCount = 0;
        while (!(columns = query.execute().getResult()).isEmpty()) {
        	for (Column<Long> c : columns) {
        		colCount++;
        	}
        	pageCount++;
        }

        Assert.assertTrue("PageCount: " + pageCount, pageCount == 3);
        Assert.assertTrue("colCount = " + colCount,colCount == 21);

        query = keyspace
        		.prepareQuery(CF_LONGCOLUMN)
        		.getKey("A")
        		.autoPaginate(true)
        		.withColumnRange(
        				new CqlRangeBuilder<Long>()
        				.setStart(-5L)
        				.setEnd(11L)
        				.setFetchSize(pageSize).build());

        pageCount = 0;
        colCount = 0;
        while (!(columns = query.execute().getResult()).isEmpty()) {
        	for (Column<Long> c : columns) {
        		colCount++;
        	}
        	pageCount++;
        }

        Assert.assertTrue(pageCount == 2);
        Assert.assertTrue("colCount = " + colCount,colCount == 15);
    }
}
