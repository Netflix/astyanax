/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.consistency.ConsistencyLevelPolicy;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Batch mutator which operates at the row level assuming the hierarchy:
 * 
 * RowKey -> ColumnFamily -> Mutation.
 * 
 * This hierarchy serves two purposes. First, it makes it possible to perform
 * multiple operations on the same row without having to repeat specifying the
 * row key. Second, it mirrors the underlying Thrift data structure which averts
 * unnecessary operations to convert from one data structure to another.
 * 
 * The mutator is not thread safe
 * 
 * If successful, all the mutations are cleared and new mutations may be
 * created. Any previously acquired ColumnFamilyMutations are no longer valid
 * and should be discarded.
 * 
 * No data is actually returned after a mutation is executed, hence the Void
 * return value type.
 * 
 * Example:
 * 
 * <pre>
 * {
 *     &#064;code
 *     ColumnFamily&lt;String, String&gt; cf = AFactory.makeColumnFamily(&quot;COLUMN_FAMILY_NAME&quot;, // Name
 *                                                                                       // of
 *                                                                                       // CF
 *                                                                                       // in
 *                                                                                       // Cassandra
 *             StringSerializer.get(), // Row key serializer (implies string type)
 *             StringSerializer.get(), // Column name serializer (implies string
 *                                     // type)
 *             ColumnType.STANDARD); // This is a standard row
 * 
 *     // Create a batch mutation
 *     RowMutationBatch m = keyspace.prepareMutationBatch();
 * 
 *     // Start mutate a column family for a specific row key
 *     ColumnFamilyMutation&lt;String&gt; cfm = m.row(cfSuper, &quot;UserId&quot;).putColumn(&quot;Address&quot;, &quot;976 Elm St.&quot;)
 *             .putColumn(&quot;Age&quot;, 50).putColumn(&quot;Gender&quot;, &quot;Male&quot;);
 * 
 *     // To delete a row
 *     m.row(cfSuper, &quot;UserId&quot;).delete();
 * 
 *     // Finally, execute the query
 *     m.execute();
 * 
 * }
 * </pre>
 * 
 * @author elandau
 * 
 * @param <K>
 */
public interface MutationBatch extends Execution<Void> {
    /**
     * Mutate a row. The ColumnFamilyMutation is only valid until execute() or
     * discardMutations is called.
     * 
     * @param rowKey
     * @return
     */
    <K, C> ColumnListMutation<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey);

    /**
     * Delete the row for all the specified column families
     * 
     * @param columnFamilies
     */
    <K> void deleteRow(Collection<ColumnFamily<K, ?>> columnFamilies, K rowKey);

    /**
     * Discard any pending mutations. All previous references returned by row
     * are now invalid.
     */
    void discardMutations();

    /**
     * Perform a shallow merge of mutations from another batch.
     * 
     * @throws UnsupportedOperationException
     *             if the other mutation is of a different type
     */
    void mergeShallow(MutationBatch other);

    /**
     * Returns true if there are no rows in the mutation. May return a false
     * true if a row() was added by calling the above row() method but no
     * mutations were created.
     * 
     * @return
     */
    boolean isEmpty();

    /**
     * Returns the number of rows being mutated
     * 
     * @return
     */
    int getRowCount();

    /**
     * Return a mapping of column families to rows being modified
     * 
     * @return
     */
    Map<ByteBuffer, Set<String>> getRowKeys();

    /**
     * Pin this operation to a specific host
     * 
     * @param host
     * @return
     */
    MutationBatch pinToHost(Host host);

    /**
     * Set the consistency level policy for this mutation (same as withConsistencyLevelPolicy)
     * 
     * @param consistencyLevelPolicy
     */
    MutationBatch setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy);

    /**
     * Set the consistency level policy for this mutation (same as setConsistencyLevelPolicy)
     * 
     * @param consistencyLevelPolicy
     * @return
     */
    MutationBatch withConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy);
    
    /**
     * Set the retry policy to use instead of the one specified in the
     * configuration
     * 
     * @param retry
     * @return
     */
    MutationBatch withRetryPolicy(RetryPolicy retry);

    /**
     * Specify a write ahead log implementation to use for this mutation
     * 
     * @param manager
     * @return
     */
    MutationBatch usingWriteAheadLog(WriteAheadLog manager);

    /**
     * Force all future mutations to have the same timestamp. Make sure to call
     * lockTimestamp before doing any other operations otherwise previously
     * created withRow mutations will use the previous timestamp.
     * 
     * @return
     */
    MutationBatch lockCurrentTimestamp();

    /**
     * This never really did anything :)
     * 
     * @param
     */
    @Deprecated
    MutationBatch setTimeout(long timeout);

    /**
     * Set the timestamp for all subsequent operations on this mutation
     * (same as withTimestamp)
     * 
     * @param timestamp
     * @return
     */
    MutationBatch setTimestamp(long timestamp);

    /**
     * Set the timestamp for all subsequent operations on this mutation.
     * 
     * @param timestamp
     * @return
     */
    MutationBatch withTimestamp(long timestamp);
    
    /**
     * Serialize the entire mutation batch into a ByteBuffer.
     * 
     * @return
     * @throws Exception
     */
    ByteBuffer serialize() throws Exception;

    /**
     * Re-recreate a mutation batch from a serialized ByteBuffer created by a
     * call to serialize(). Serialization of MutationBatches from different
     * implementations is not guaranteed to match.
     * 
     * @param data
     * @throws Exception
     */
    void deserialize(ByteBuffer data) throws Exception;

}
